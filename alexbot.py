import time
import logging
from typing import Dict, Any

import psycopg2
from binance.client import Client
from binance import ThreadedWebsocketManager

from config import (
    BINANCE_API_KEY, BINANCE_API_SECRET,
    MIRROR_ENABLED, MIRROR_B_API_KEY, MIRROR_B_API_SECRET,
    MIRROR_COEFFICIENT
)
from db import (
    pg_conn, pg_raw,
    pg_upsert_position, pg_delete_position, pg_get_position,
    wipe_mirror, reset_pending
)
from telegram_bot import tg_a, tg_m

log = logging.getLogger(__name__)

CHILD_TYPES = {
    "STOP","STOP_MARKET","STOP_LOSS","STOP_LOSS_LIMIT",
    "TAKE_PROFIT","TAKE_PROFIT_LIMIT","TAKE_PROFIT_MARKET"
}

def pos_color(side:str) -> str:
    return "🟢" if side=="LONG" else "🔴"

def child_color() -> str:
    return "🔵"

def decode_side(o: Dict[str,Any]) -> str:
    """
    Определяем, LONG это или SHORT, учитывая reduceOnly (R).
    """
    reduce_flag = bool(o.get("R", False))
    raw_side    = o["S"]  # "BUY" / "SELL"
    if reduce_flag:
        # Закрываем / уменьшаем
        if raw_side == "BUY":
            return "SHORT"
        else:
            return "LONG"
    else:
        # Открываем / увеличиваем
        if raw_side == "BUY":
            return "LONG"
        else:
            return "SHORT"

def reason_text(otype: str) -> str:
    """
    "(MARKET)", "(LIMIT)", "(STOP_MARKET)" и т.п.
    """
    mp = {
        "MARKET": "(MARKET)",
        "LIMIT": "(LIMIT)",
        "STOP": "(STOP)",
        "STOP_MARKET": "(STOP MARKET)",
        "TAKE_PROFIT": "(TAKE PROFIT)",
        "TAKE_PROFIT_MARKET": "(TAKE PROFIT MARKET)"
    }
    return mp.get(otype, f"({otype})")

def _fmt_float(x: float, digits: int = 4) -> str:
    """
    Универсальное форматирование x на `digits` знаков (для PNL, баланса).
    Убираем лишние нули в конце.
    """
    s = f"{x:.{digits}f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s

class AlexBot:
    def __init__(self):
        log.debug("AlexBot.__init__ called")

        self.client_a = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        self.client_b = (
            Client(MIRROR_B_API_KEY, MIRROR_B_API_SECRET)
            if MIRROR_ENABLED else None
        )

        # Пример: словари для тик‑сайза (если нужно)
        self.lot_size_map = {}
        self.price_size_map = {}
        self._init_symbol_precisions()

        # Запуск WebSocket
        self.ws = ThreadedWebsocketManager(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET
        )
        self.ws.start()
        self.ws.start_futures_user_socket(callback=self._ws_handler)

        # Первоначальная очистка / синхронизация
        wipe_mirror()
        reset_pending()
        self._sync_start()

        # Приветствие с балансами
        self._hello()

    # ---------------- Пример форматирования qty/price ----------------
    def _init_symbol_precisions(self):
        """
        Если нужно — запрашиваем binance.futures_exchange_info()
        и формируем lot_size_map, price_size_map.
        """
        pass  # Реализуйте при необходимости

    def _fmt_qty(self, symbol: str, qty: float) -> str:
        """Упрощённый пример: 3 знака"""
        return f"{qty:.3f}"

    def _fmt_price(self, symbol: str, price: float) -> str:
        """Упрощённый пример: 2 знака"""
        return f"{price:.2f}"
    # -----------------------------------------------------------------

    def _hello(self):
        """
        Приветствие + баланс.
        Выводим в лог (консоль) и шлём в зеркальный чат (tg_m).
        """
        bal_main = self._usdt(self.client_a)
        msg = f"▶️  Бот запущен.\nОсновной аккаунт: {_fmt_float(bal_main)} USDT"

        if self.client_b and MIRROR_ENABLED:
            bal_mirr = self._usdt(self.client_b)
            msg += f"\nЗеркальный аккаунт активен: {_fmt_float(bal_mirr)} USDT"

        # Пишем в лог
        log.info(msg)
        # Посылаем в зеркальный чат (как и было в примере)
        tg_m(msg)

    def _usdt(self, client: Client) -> float:
        """Возвращаем баланс USDT."""
        try:
            bals = client.futures_account_balance()
            for b in bals:
                if b["asset"]=="USDT":
                    return float(b["balance"])
        except Exception as e:
            log.error("_usdt: %s", e)
        return 0.0

    def _ws_handler(self, msg: Dict[str,Any]):
        pg_raw(msg)
        log.debug("[WS] %s", msg)
        if msg.get("e") == "ORDER_TRADE_UPDATE":
            self._on_order(msg["o"])
            self._diff_positions()

    def _on_order(self, o: Dict[str,Any]):
        """
        Обработка события ORDER_TRADE_UPDATE.
        Если STOP/TAKE FILLED => печатаем "STOP активирован", затем идём к логике reduceOnly.
        """
        sym    = o["s"]
        otype  = o["ot"]         # MARKET/LIMIT/STOP...
        status = o["X"]          # NEW/FILLED/CANCELED...
        fill_price = float(o.get("ap", 0))  # последняя цена исполнения
        fill_qty   = float(o.get("l", 0))   # объём исполнения
        reduce_flag= bool(o.get("R", False))
        side       = decode_side(o)

        # Реализованный PnL, который Binance сам считает (учитывая комиссию)
        partial_pnl = float(o.get("rp", 0.0))
        rtxt = reason_text(otype)

        # --- CANCEL LIMIT ---
        if otype=="LIMIT" and status=="CANCELED":
            price = float(o.get("p", 0))
            qty   = float(o.get("q", 0))
            txt = (f"🔵 Trader: {sym} LIMIT отменен. "
                   f"(Был {pos_color(side)} {side}, Объём: {self._fmt_qty(sym, qty)} "
                   f"по цене {self._fmt_price(sym, price)}).")
            tg_a(txt)
            pg_delete_position("positions", sym, side)
            if MIRROR_ENABLED:
                pg_delete_position("mirror_positions", sym, side)
                tg_m(f"[Mirror]: {pos_color(side)} Trader: {sym} LIMIT отменён (mirror).")
            return

        # --- NEW LIMIT ---
        if otype=="LIMIT" and status=="NEW":
            price = float(o.get("p", 0))
            qty   = float(o.get("q", 0))
            pg_upsert_position("positions", sym, side, 0.0, 0.0, 0.0, "binance", True)
            txt = (f"🔵 Trader: {sym} Новый LIMIT {pos_color(side)} {side}. "
                   f"Объём: {self._fmt_qty(sym, qty)} по цене {self._fmt_price(sym, price)}.")
            tg_a(txt)
            return

        # --- NEW STOP/TAKE ---
        if otype in CHILD_TYPES and status=="NEW":
            trg = float(o.get("sp") or o.get("p") or 0)
            if trg:
                kind = "STOP" if "STOP" in otype else "TAKE"
                tg_a(f"{child_color()} Trader: {sym} {kind} установлен на цену {self._fmt_price(sym, trg)}")
            return

        # ================= FILLED =================
        if status == "FILLED":
            if fill_qty < 1e-12:
                return
            old_amt, old_entry, old_rpnl = pg_get_position("positions", sym, side) or (0.0, 0.0, 0.0)
            new_rpnl = old_rpnl + partial_pnl

            # ---- (1) Если это STOP/TAKE, пишем "активирован" ----
            if otype in CHILD_TYPES:
                kind = "STOP" if "STOP" in otype else "TAKE"
                trigger_price = float(o.get("sp", 0))
                tg_a(
                    f"{child_color()} Trader: {sym} {kind} активирован по цене {self._fmt_price(sym, trigger_price)} "
                    f"(факт. исполнение {self._fmt_price(sym, fill_price)})"
                )

            # ---- (2) Основная логика reduce-only или открытия ----
            if reduce_flag:
                # закрытие / уменьшение
                new_amt = old_amt - fill_qty
                ratio_close = (fill_qty / old_amt)*100 if old_amt>1e-12 else 100
                if ratio_close>100:
                    ratio_close=100

                if new_amt <= 1e-8:
                    # полное закрытие
                    txt = (
                        f"{pos_color(side)} Trader: {sym} полное закрытие позиции {side} "
                        f"({int(round(ratio_close))}%, {self._fmt_qty(sym, old_amt)} --> 0) "
                        f"по цене {self._fmt_price(sym, fill_price)}, общий PNL: {_fmt_float(new_rpnl)}"
                    )
                    tg_a(txt)
                    pg_delete_position("positions", sym, side)
                else:
                    # частичное
                    txt = (
                        f"{pos_color(side)} Trader: {sym} частичное закрытие позиции {side} "
                        f"({int(round(ratio_close))}%, {self._fmt_qty(sym, old_amt)} --> {self._fmt_qty(sym, new_amt)}) "
                        f"по цене {self._fmt_price(sym, fill_price)}, текущий PNL: {_fmt_float(new_rpnl)}"
                    )
                    tg_a(txt)
                    pg_upsert_position("positions", sym, side, new_amt, old_entry, new_rpnl, "binance", False)

                # зеркальное закрытие
                if MIRROR_ENABLED:
                    self._mirror_reduce(sym, side, fill_qty, fill_price, partial_pnl)

            else:
                # Открытие / увеличение
                new_amt = old_amt + fill_qty
                if old_amt < 1e-12:
                    # новая позиция
                    txt = (
                        f"{pos_color(side)} Trader: {sym} Открыта позиция {side} {rtxt} "
                        f"на {self._fmt_qty(sym, fill_qty)} "
                        f"по цене {self._fmt_price(sym, fill_price)}"
                    )
                else:
                    ratio_inc = (fill_qty / old_amt)*100 if old_amt>1e-12 else 100
                    if ratio_inc>100:
                        ratio_inc=100
                    txt = (
                        f"{pos_color(side)} Trader: {sym} Увеличение позиции {side} "
                        f"({int(round(ratio_inc))}%, {self._fmt_qty(sym, old_amt)} --> {self._fmt_qty(sym, new_amt)}) "
                        f"{rtxt} по цене {self._fmt_price(sym, fill_price)}"
                    )
                tg_a(txt)
                pg_upsert_position("positions", sym, side, new_amt, fill_price, new_rpnl, "binance", False)

                if MIRROR_ENABLED:
                    self._mirror_increase(sym, side, fill_qty, fill_price, rtxt)

    def _mirror_reduce(self, sym: str, side: str, fill_qty: float, fill_price: float, partial_pnl: float):
        """
        Зеркальное уменьшение / закрытие.
        """
        old_m_amt, old_m_entry, old_m_rpnl = pg_get_position("mirror_positions", sym, side) or (0.0, 0.0, 0.0)
        reduce_qty = fill_qty * MIRROR_COEFFICIENT
        new_m_pnl  = old_m_rpnl + partial_pnl * MIRROR_COEFFICIENT
        new_m_amt  = old_m_amt - reduce_qty

        ratio_close = (reduce_qty / old_m_amt)*100 if old_m_amt>1e-12 else 100
        if ratio_close>100:
            ratio_close=100

        side_binance = "BUY" if side=="SHORT" else "SELL"
        try:
            self.client_b.futures_create_order(
                symbol=sym,
                side=side_binance,
                type="MARKET",
                quantity=reduce_qty,
                reduceOnly=True
            )
        except Exception as e:
            log.error("_mirror_reduce: create_order error: %s", e)

        if new_m_amt <= 1e-8:
            pg_delete_position("mirror_positions", sym, side)
            tg_m((
                f"[Mirror]: {pos_color(side)} Trader: {sym} полное закрытие {side} "
                f"({int(round(ratio_close))}%, {self._fmt_qty(sym, old_m_amt)} --> 0.0) "
                f"по цене {self._fmt_price(sym, fill_price)}, PNL: {_fmt_float(new_m_pnl)}"
            ))
        else:
            pg_upsert_position("mirror_positions", sym, side, new_m_amt, old_m_entry, new_m_pnl, "mirror", False)
            tg_m((
                f"[Mirror]: {pos_color(side)} Trader: {sym} частичное закрытие {side} "
                f"({int(round(ratio_close))}%, {self._fmt_qty(sym, old_m_amt)} --> {self._fmt_qty(sym, new_m_amt)}) "
                f"по цене {self._fmt_price(sym, fill_price)}, PNL: {_fmt_float(new_m_pnl)}"
            ))

    def _mirror_increase(self, sym: str, side: str, fill_qty: float, fill_price: float, rtxt: str):
        """
        Зеркальное открытие / увеличение
        """
        old_m_amt, old_m_entry, old_m_rpnl = pg_get_position("mirror_positions", sym, side) or (0.0, 0.0, 0.0)
        inc_qty = fill_qty * MIRROR_COEFFICIENT
        new_m_amt = old_m_amt + inc_qty
        side_binance = "BUY" if side=="LONG" else "SELL"

        try:
            self.client_b.futures_create_order(
                symbol=sym,
                side=side_binance,
                type="MARKET",
                quantity=inc_qty
            )
        except Exception as e:
            log.error("_mirror_increase: create_order error: %s", e)

        new_m_pnl = old_m_rpnl
        pg_upsert_position("mirror_positions", sym, side, new_m_amt, fill_price, new_m_pnl, "mirror", False)

        if old_m_amt < 1e-12:
            tg_m((
                f"[Mirror]: {pos_color(side)} Trader: {sym} Открыта позиция {side} {rtxt} "
                f"на {self._fmt_qty(sym, inc_qty)}, Цена: {self._fmt_price(sym, fill_price)}, PNL: {_fmt_float(old_m_rpnl)}"
            ))
        else:
            ratio_inc = (inc_qty / old_m_amt)*100 if old_m_amt>1e-12 else 100
            if ratio_inc>100:
                ratio_inc=100
            tg_m((
                f"[Mirror]: {pos_color(side)} Trader: {sym} Увеличение позиции {side} "
                f"({int(round(ratio_inc))}%, {self._fmt_qty(sym, old_m_amt)} --> {self._fmt_qty(sym, new_m_amt)}) "
                f"{rtxt} по цене {self._fmt_price(sym, fill_price)}, PNL: {_fmt_float(old_m_rpnl)}"
            ))

    def _diff_positions(self):
        """
        Заглушка: если нужно, периодически сверять фактические позиции и БД.
        """
        log.debug("_diff_positions called")

    def _sync_start(self):
        """
        Синхронизация при старте: смотрим реальные позиции, лимиты,
        добавляем в БД, удаляем лишнее.
        """
        log.debug("_sync_start called")
        # … ваш код из предыдущих версий (futures_position_information, futures_get_open_orders)
        pass

    def run(self):
        log.debug("AlexBot.run called")
        try:
            # Печатаем в консоль
            log.info("[Main] bot running ... Ctrl+C to stop")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            tg_m("⏹️  Бот остановлен пользователем")
        finally:
            self.ws.stop()
            log.info("[Main] bye.")