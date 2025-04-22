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
    Определить, какая это сторона позиции (LONG/SHORT),
    учитывая поле reduceOnly (R).
    """
    reduce_flag = bool(o.get("R", False))
    raw_side    = o["S"]  # "BUY" / "SELL"
    if reduce_flag:
        # Закрываем/уменьшаем позицию
        if raw_side == "BUY":
            return "SHORT"  # closing a short
        else:
            return "LONG"   # closing a long
    else:
        # Обычный ордер на открытие/увеличение
        if raw_side == "BUY":
            return "LONG"
        else:
            return "SHORT"

def reason_text(otype: str) -> str:
    """
    Возвращает "(MARKET)" или "(LIMIT)" и т.д.
    для понятного описания типа ордера.
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
    Универсальное форматирование на `digits` знаков (например, для PNL).
    """
    return f"{x:.{digits}f}".rstrip('0').rstrip('.') if '.' in f"{x:.{digits}f}" else f"{x:.{digits}f}"

class AlexBot:
    def __init__(self):
        log.debug("AlexBot.__init__ called")

        self.client_a = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        self.client_b = (
            Client(MIRROR_B_API_KEY, MIRROR_B_API_SECRET)
            if MIRROR_ENABLED else None
        )

        # Словари для форматирования
        self.lot_size_map   = {}  # symbol -> int (знаков для qty)
        self.price_size_map = {}  # symbol -> int (знаков для price)

        # Сначала инициализируем словари точности
        self._init_symbol_precisions()

        # Запуск WS
        self.ws = ThreadedWebsocketManager(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET
        )
        self.ws.start()
        self.ws.start_futures_user_socket(callback=self._ws_handler)

        # Инициализация
        wipe_mirror()
        reset_pending()
        self._sync_start()
        self._hello()

    # ────────────────────────── Форматирование чисел ──────────────────────────
    def _init_symbol_precisions(self):
        """
        Один раз при старте бота запрашиваем биржевую инфу (futures_exchange_info).
        Парсим stepSize (LOT_SIZE) -> lot_size_map.
        Парсим tickSize (PRICE_FILTER) -> price_size_map.
        """
        log.debug("_init_symbol_precisions called")
        try:
            info = self.client_a.futures_exchange_info()
            for s in info["symbols"]:
                sym_name = s["symbol"]  # например "BTCUSDT"
                lot_dec = 4   # дефолтная точность qty
                price_dec = 4 # дефолтная точность price

                # Парсим filters
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        step_str = f["stepSize"]  # '0.00000100'
                        lot_dec  = self._step_to_decimals(step_str)
                    elif f["filterType"] == "PRICE_FILTER":
                        tick_str = f["tickSize"]  # '0.01'
                        price_dec = self._step_to_decimals(tick_str)

                self.lot_size_map[sym_name]   = lot_dec
                self.price_size_map[sym_name] = price_dec

            log.info("Symbol precisions loaded. Example: BTCUSDT => qty_dec=%d, price_dec=%d",
                     self.lot_size_map.get("BTCUSDT", 4),
                     self.price_size_map.get("BTCUSDT", 4))
        except Exception as e:
            log.error("_init_symbol_precisions error: %s", e)

    @staticmethod
    def _step_to_decimals(step_str: str) -> int:
        """
        Преобразуем '0.00000100' -> 6
        '0.01' -> 2
        '1' -> 0
        """
        s = step_str.rstrip('0')
        if '.' not in s:
            return 0
        return len(s.split('.')[1])

    def _fmt_qty(self, symbol: str, qty: float) -> str:
        """
        Форматируем объём (qty) с учётом lot_size_map
        """
        dec = self.lot_size_map.get(symbol, 4)
        val = f"{qty:.{dec}f}"
        # Уберём trailing zeros
        return val.rstrip('0').rstrip('.') if '.' in val else val

    def _fmt_price(self, symbol: str, price: float) -> str:
        """
        Форматируем цену с учётом price_size_map
        """
        dec = self.price_size_map.get(symbol, 4)
        val = f"{price:.{dec}f}"
        return val.rstrip('0').rstrip('.') if '.' in val else val

    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _calc_pnl(side: str, entry: float, exit_p: float, qty: float) -> float:
        """
        Простейший PnL = (exit - entry) * qty * sign
        side=="LONG" => +1, side=="SHORT" => -1
        """
        sign = 1 if side=="LONG" else -1
        return (exit_p - entry) * qty * sign

    def _ws_handler(self, msg: Dict[str,Any]):
        """
        Callback на каждое WS-сообщение:
         - Сохраняем "сырое" в БД
         - Логируем (debug)
         - Если ORDER_TRADE_UPDATE -> _on_order + _diff_positions
        """
        pg_raw(msg)
        log.debug("[WS] %s", msg)

        if msg.get("e") == "ORDER_TRADE_UPDATE":
            o = msg["o"]
            self._on_order(o)
            self._diff_positions()

    def _on_order(self, o: Dict[str,Any]):
        """
        Обработка ордеров.
        При полном закрытии/снятии ордера — удаляем запись из positions.
        Зеркалируем любые изменения.
        """
        otype  = o["ot"]        # "LIMIT", "MARKET", ...
        status = o["X"]         # "NEW", "FILLED", "CANCELED", ...
        fill_price = float(o.get("ap", 0))  # средняя цена исполнения
        fill_qty   = float(o.get("l", 0))   # исполненная часть в этом ивенте
        reduce_flag= bool(o.get("R", False))
        sym        = o["s"]
        side       = decode_side(o)

        rtxt = reason_text(otype)  # (MARKET), (LIMIT)...

        # --- CANCELED LIMIT ---
        if otype=="LIMIT" and status=="CANCELED":
            price = float(o.get("p", 0))
            qty   = float(o.get("q", 0))
            txt = (f"🔵 Trader: {sym} LIMIT отменен. "
                   f"(Был {pos_color(side)} {side}, Объём: {self._fmt_qty(sym, qty)} "
                   f"по цене {self._fmt_price(sym, price)}).")
            tg_a(txt)

            pg_delete_position("positions", sym, side)
            if MIRROR_ENABLED and self.client_b:
                pg_delete_position("mirror_positions", sym, side)
                tg_m(f"[Mirror]: {pos_color(side)} Trader: {sym} LIMIT отменён (mirror).")
            return

        # --- NEW LIMIT ---
        if otype=="LIMIT" and status=="NEW":
            price = float(o.get("p", 0))
            qty   = float(o.get("q", 0))
            # Запишем amt=0, pending=True
            pg_upsert_position("positions", sym, side, 0.0, 0.0, 0.0, "binance", True)
            txt = (f"🔵 Trader: {sym} Новый LIMIT {pos_color(side)} {side}. "
                   f"Объём: {self._fmt_qty(sym, qty)} по цене {self._fmt_price(sym, price)}.")
            tg_a(txt)
            return

        # --- NEW child STOP/TAKE ---
        if otype in CHILD_TYPES and status=="NEW":
            trg = float(o.get("sp") or o.get("p") or 0)
            if trg:
                kind = "STOP" if "STOP" in otype else "TAKE"
                tg_a(f"{child_color()} Trader: {sym} {kind} установлен на цену {self._fmt_price(sym, trg)}")
            return

        # --- FILLED (MARKET, LIMIT, STOP...) ---
        if status == "FILLED":
            if fill_qty < 1e-12:
                # Если qty=0, не обрабатываем
                return

            old_pos = pg_get_position("positions", sym, side) or (0.0, 0.0, 0.0)
            old_amt, old_entry, old_rpnl = old_pos

            if reduce_flag:
                # ========== УМЕНЬШЕНИЕ / ЗАКРЫТИЕ ===========
                partial_pnl = self._calc_pnl(side, old_entry, fill_price, fill_qty)
                new_rpnl = old_rpnl + partial_pnl
                new_amt = old_amt - fill_qty

                ratio_close = (fill_qty / old_amt)*100 if old_amt>1e-12 else 100

                if new_amt <= 1e-8:
                    # Полное закрытие
                    new_amt = 0.0
                    txt = (f"{pos_color(side)} Trader: {sym} полное закрытие позиции {side} "
                           f"({round(ratio_close)}%, {self._fmt_qty(sym, old_amt)} --> {self._fmt_qty(sym, new_amt)}) "
                           f"по цене {self._fmt_price(sym, fill_price)}, общий PNL: {_fmt_float(new_rpnl)}")
                    tg_a(txt)
                    pg_delete_position("positions", sym, side)
                else:
                    # Частичное
                    txt = (f"{pos_color(side)} Trader: {sym} частичное закрытие позиции {side} "
                           f"({round(ratio_close)}%, {self._fmt_qty(sym, old_amt)} --> {self._fmt_qty(sym, new_amt)}) "
                           f"по цене {self._fmt_price(sym, fill_price)}, текущий PNL: {_fmt_float(new_rpnl)}")
                    tg_a(txt)
                    pg_upsert_position("positions", sym, side, new_amt, old_entry, new_rpnl, "binance", False)

                # Зеркало
                if MIRROR_ENABLED:
                    self._mirror_reduce(sym, side, fill_qty, fill_price, partial_pnl)
            else:
                # ========== ОТКРЫТИЕ / УВЕЛИЧЕНИЕ ===========
                new_amt = old_amt + fill_qty

                if old_amt < 1e-12:
                    # Открытие
                    txt = (f"{pos_color(side)} Trader: {sym} Открыта позиция {side} {rtxt} "
                           f"на {self._fmt_qty(sym, fill_qty)} "
                           f"по цене {self._fmt_price(sym, fill_price)}")
                else:
                    ratio_inc = (fill_qty / old_amt)*100 if old_amt>1e-12 else 100
                    txt = (f"{pos_color(side)} Trader: {sym} Увеличение позиции {side} "
                           f"({round(ratio_inc)}%, {self._fmt_qty(sym, old_amt)} --> {self._fmt_qty(sym, new_amt)}) "
                           f"{rtxt} по цене {self._fmt_price(sym, fill_price)}")

                tg_a(txt)
                pg_upsert_position("positions", sym, side, new_amt, fill_price, old_rpnl, "binance", False)

                # Зеркало
                if MIRROR_ENABLED:
                    self._mirror_increase(sym, side, fill_qty, fill_price, rtxt)

    def _mirror_reduce(self, sym: str, side: str, fill_qty: float, fill_price: float, partial_pnl: float):
        """
        Зеркальное уменьшение / закрытие (reduce-only).
        """
        old_m = pg_get_position("mirror_positions", sym, side) or (0.0, 0.0, 0.0)
        old_m_amt, old_m_entry, old_m_rpnl = old_m

        reduce_qty = fill_qty * MIRROR_COEFFICIENT
        partial_m_pnl = partial_pnl * MIRROR_COEFFICIENT
        new_m_pnl = old_m_rpnl + partial_m_pnl
        new_m_amt = old_m_amt - reduce_qty

        ratio_close = (reduce_qty / old_m_amt)*100 if old_m_amt>1e-12 else 100

        # side_binance: если side="SHORT", для закрытия нужно "BUY" reduceOnly
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
            new_m_amt = 0.0
            pg_delete_position("mirror_positions", sym, side)
            tg_m((f"[Mirror]: {pos_color(side)} Trader: {sym} полное закрытие {side} "
                  f"({round(ratio_close)}%, {self._fmt_qty(sym, old_m_amt)} --> 0.0) "
                  f"по цене {self._fmt_price(sym, fill_price)}, PNL: {_fmt_float(new_m_pnl)}"))
        else:
            pg_upsert_position("mirror_positions", sym, side, new_m_amt, old_m_entry, new_m_pnl, "mirror", False)
            tg_m((f"[Mirror]: {pos_color(side)} Trader: {sym} частичное закрытие {side} "
                  f"({round(ratio_close)}%, {self._fmt_qty(sym, old_m_amt)} --> {self._fmt_qty(sym, new_m_amt)}) "
                  f"по цене {self._fmt_price(sym, fill_price)}, PNL: {_fmt_float(new_m_pnl)}"))

    def _mirror_increase(self, sym: str, side: str, fill_qty: float, fill_price: float, rtxt: str):
        """
        Зеркальное открытие / увеличение
        """
        old_m = pg_get_position("mirror_positions", sym, side) or (0.0, 0.0, 0.0)
        old_m_amt, old_m_entry, old_m_rpnl = old_m
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

        pg_upsert_position("mirror_positions", sym, side, new_m_amt, fill_price, old_m_rpnl, "mirror", False)

        if old_m_amt < 1e-12:
            # Открытие
            tg_m(f"[Mirror]: {pos_color(side)} Trader: {sym} Открыта позиция {side} {rtxt} "
                 f"на {self._fmt_qty(sym, inc_qty)}, Цена: {self._fmt_price(sym, fill_price)}, "
                 f"PNL: {_fmt_float(old_m_rpnl)}")
        else:
            ratio_inc = (inc_qty / old_m_amt)*100 if old_m_amt>1e-12 else 100
            tg_m(f"[Mirror]: {pos_color(side)} Trader: {sym} Увеличение позиции {side} "
                 f"({round(ratio_inc)}%, {self._fmt_qty(sym, old_m_amt)} --> {self._fmt_qty(sym, new_m_amt)}) "
                 f"{rtxt} по цене {self._fmt_price(sym, fill_price)}, PNL: {_fmt_float(old_m_rpnl)}")

    def _diff_positions(self):
        """
        Заглушка: периодически можно синхронизировать
        """
        log.debug("_diff_positions called")

    def _sync_start(self):
        """
        При старте бот синхронизирует позиции/ордера с биржей.
        """
        log.debug("_sync_start called")
        try:
            # Открытые позиции
            positions = self.client_a.futures_position_information()
            real_symbols = set()
            for p in positions:
                amt = float(p["positionAmt"])
                if abs(amt) < 1e-12:
                    continue
                sym   = p["symbol"]
                side  = "LONG" if amt>0 else "SHORT"
                price = float(p["entryPrice"])
                vol   = abs(amt)
                real_symbols.add((sym, side))

                txt = (f"{pos_color(side)} (start) Trader: {sym} "
                       f"Открыта {side} Объём: {self._fmt_qty(sym, vol)}, "
                       f"Цена: {self._fmt_price(sym, price)}")
                open_orders = self.client_a.futures_get_open_orders(symbol=sym)
                sl = tp = None
                for od in open_orders:
                    if od["type"] in CHILD_TYPES and od["status"]=="NEW":
                        trg = float(od.get("stopPrice") or od.get("price") or 0)
                        if trg:
                            if "STOP" in od["type"]:
                                sl = trg
                            else:
                                tp = trg
                if sl:
                    txt += f", SL={self._fmt_price(sym, sl)}"
                if tp:
                    txt += f", TP={self._fmt_price(sym, tp)}"
                tg_a(txt)

                pg_upsert_position("positions", sym, side, vol, price, 0.0, "binance", False)

            # LIMIT‑ордера
            orders = self.client_a.futures_get_open_orders()
            real_limits = set()
            for od in orders:
                if od["type"]=="LIMIT" and od["status"]=="NEW":
                    sym   = od["symbol"]
                    price = float(od["price"])
                    qty   = float(od["origQty"])
                    side  = "LONG" if od["side"]=="BUY" else "SHORT"
                    real_limits.add((sym, side))

                    pg_upsert_position("positions", sym, side, 0.0, 0.0, 0.0, "binance", True)
                    txt = (f"🔵 (start) Trader: {sym} Новый LIMIT {pos_color(side)} {side}. "
                           f"Объём: {self._fmt_qty(sym, qty)} по цене {self._fmt_price(sym, price)}.")
                    # SL/TP?
                    sl = tp = None
                    for ch in orders:
                        if (ch["symbol"]==sym and ch["status"]=="NEW"
                                and ch["orderId"]!=od["orderId"]
                                and ch["type"] in CHILD_TYPES):
                            trg = float(ch.get("stopPrice") or ch.get("price") or 0)
                            if trg:
                                if "STOP" in ch["type"]:
                                    sl = trg
                                else:
                                    tp = trg
                    if sl:
                        txt += f" SL={self._fmt_price(sym, sl)}"
                    if tp:
                        txt += f" TP={self._fmt_price(sym, tp)}"
                    tg_a(txt)

            # Удалить «лишнее» из БД
            with pg_conn() as conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol, position_side, position_amt, pending
                      FROM public.positions
                     WHERE exchange='binance'
                """)
                rows = cur.fetchall()
                for (db_sym, db_side, db_amt, db_pending) in rows:
                    db_key = (db_sym, db_side)
                    if db_pending:
                        if db_key not in real_limits:
                            log.info("Removing old LIMIT from DB: %s, %s", db_sym, db_side)
                            pg_delete_position("positions", db_sym, db_side)
                    else:
                        if db_key not in real_symbols:
                            log.info("Removing old POSITION from DB: %s, %s", db_sym, db_side)
                            pg_delete_position("positions", db_sym, db_side)

        except Exception as e:
            log.error("_sync_start positions: %s", e)

    def _hello(self):
        """
        Приветствие + баланс
        """
        log.debug("_hello called")
        bal_a = self._usdt(self.client_a)
        msg   = f"▶️  Бот запущен.\nОсновной аккаунт: {_fmt_float(bal_a)} USDT"
        if self.client_b:
            bal_b = self._usdt(self.client_b)
            msg  += f"\nЗеркальный аккаунт активен: {_fmt_float(bal_b)} USDT"
        tg_m(msg)

    @staticmethod
    def _usdt(client: Client) -> float:
        log.debug("_usdt called")
        try:
            balances = client.futures_account_balance()
            for b in balances:
                if b["asset"]=="USDT":
                    return float(b["balance"])
        except Exception as e:
            log.error("_usdt: %s", e)
        return 0.0

    def run(self):
        log.debug("AlexBot.run called")
        try:
            log.info("[Main] bot running ... Ctrl+C to stop")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            tg_m("⏹️  Бот остановлен пользователем")
        finally:
            self.ws.stop()
            log.info("[Main] bye.")