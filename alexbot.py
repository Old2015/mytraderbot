import time
import logging
from typing import Dict, Any

import psycopg2  # нужно для запроса в БД
from binance.client import Client
from binance import ThreadedWebsocketManager

from config import (
    BINANCE_API_KEY, BINANCE_API_SECRET,
    MIRROR_ENABLED, MIRROR_B_API_KEY, MIRROR_B_API_SECRET,
    MIRROR_COEFFICIENT
)
from db import (
    pg_conn, pg_raw, pg_upsert_position,
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

class AlexBot:
    def __init__(self):
        log.debug("AlexBot.__init__ called")

        self.client_a = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        self.client_b = (
            Client(MIRROR_B_API_KEY, MIRROR_B_API_SECRET)
            if MIRROR_ENABLED else None
        )

        # Запуск WebSocket
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

    @staticmethod
    def _calc_pnl(side: str, entry: float, exit_p: float, qty: float) -> float:
        """
        Простейший PnL = (exit - entry) * qty * sign
        side=="LONG" => +1
        side=="SHORT" => -1
        """
        sign = 1 if side=="LONG" else -1
        return (exit_p - entry) * qty * sign

    def _ws_handler(self, msg: Dict[str,Any]):
        """
        Callback на каждое WS-сообщение:
         - Сохраняем "сырое" в БД
         - Логируем на уровне DEBUG (чтобы не засорять консоль)
         - Если ORDER_TRADE_UPDATE -> _on_order() + _diff_positions()
        """
        pg_raw(msg)
        log.debug("[WS] %s", msg)

        if msg.get("e") == "ORDER_TRADE_UPDATE":
            o = msg["o"]
            self._on_order(o)
            self._diff_positions()

    def _on_order(self, o: Dict[str,Any]):
        """
        Обработка ордеров (MARKET/LIMIT; reduce-only или нет).
        Разделяем логику:
         1) Если reduceOnly=true => (частичное/полное) закрытие
         2) Иначе => открытие/увеличение
        """
        sym    = o["s"]        # символ, напр. "XRPUSDT"
        otype  = o["ot"]       # тип ордера (LIMIT, MARKET, STOP, TAKE_PROFIT, ...)
        status = o["X"]        # статус (NEW, FILLED, CANCELED, PARTIALLY_FILLED, ...)
        side   = "LONG" if o["S"]=="BUY" else "SHORT"
        reduce_flag = bool(o.get("R", False))  # R=true => reduceOnly

        # --- CANCELED LIMIT ---
        if otype=="LIMIT" and status=="CANCELED":
            price = float(o.get("p", 0))
            qty   = float(o.get("q", 0))
            txt = (f"🔵 Trader: {sym} LIMIT отменен. "
                   f"(Был {pos_color(side)} {side}, Объём: {qty} по цене {price}).")
            tg_a(txt)
            # Запишем в БД, что позиция = 0 (раз отменили лимит)
            pg_upsert_position("positions", sym, side, 0.0, 0.0, 0.0, "binance", False)
            return

        # --- NEW LIMIT ---
        if otype=="LIMIT" and status=="NEW":
            price = float(o.get("p", 0))
            qty   = float(o.get("q", 0))
            txt   = f"🔵 Trader: {sym} Новый LIMIT {pos_color(side)} {side}. Объём: {qty} по цене {price}."
            tg_a(txt)
            pg_upsert_position("positions", sym, side, qty, price, 0.0, "binance", True)
            return

        # --- NEW child STOP/TAKE ---
        if otype in CHILD_TYPES and status=="NEW":
            trg = float(o.get("sp") or o.get("p") or 0)
            if trg:
                kind = "STOP" if "STOP" in otype else "TAKE"
                tg_a(f"{child_color()} Trader: {sym} {kind} установлен на цену {trg}")
            return

        # --- FILLED (MARKET or LIMIT) ---
        if status == "FILLED":
            fill_price = float(o.get("ap", 0))
            fill_qty   = float(o.get("l", 0))  # исполненная часть в этом ивенте

            # 1) Извлекаем из БД старую позицию (amt, entry_price, realized_pnl)
            old_amt = 0.0
            old_entry = 0.0
            old_rpnl = 0.0
            try:
                with pg_conn() as conn, conn.cursor() as cur:
                    cur.execute("""
                        SELECT position_amt, entry_price, realized_pnl
                          FROM public.positions
                         WHERE symbol=%s AND position_side=%s
                    """, (sym, side))
                    row = cur.fetchone()
                    if row:
                        old_amt, old_entry, old_rpnl = float(row[0]), float(row[1]), float(row[2])
            except psycopg2.Error as e:
                log.error("_on_order: DB fetch error: %s", e)

            # 2) Логика reduce-only => закрытие / уменьшение
            if reduce_flag:
                partial_pnl = self._calc_pnl(side, old_entry, fill_price, fill_qty)
                new_rpnl = old_rpnl + partial_pnl
                new_amt = old_amt - fill_qty
                if new_amt < 1e-8:
                    # Полное закрытие
                    new_amt = 0.0
                    txt = (f"{pos_color(side)} Trader: {sym} полное закрытие позиции "
                           f"{side} ({old_amt} --> {new_amt}) по цене {old_entry}, "
                           f"текущий PNL: {new_rpnl:.2f}")
                else:
                    # Частичное уменьшение
                    txt = (f"{pos_color(side)} Trader: {sym} уменьшение позиции "
                           f"{side} ({old_amt} --> {new_amt}) по цене {old_entry}, "
                           f"текущий PNL: {new_rpnl:.2f}")

                tg_a(txt)
                # Запишем в БД
                pg_upsert_position(
                    "positions", sym, side,
                    new_amt,
                    old_entry if new_amt>0 else 0.0,  # если закрыли полностью => entry=0
                    new_rpnl,
                    "binance",
                    False
                )

            else:
                # 3) Открытие / увеличение
                # Проверим, была ли позиция 0 => "Открыта позиция" иначе "Увеличение"
                new_amt = old_amt + fill_qty
                if old_amt < 1e-8:
                    # полное отсутствие => новая позиция
                    txt = (f"{pos_color(side)} Trader: {sym} Открыта позиция {side} "
                           f"на {fill_qty} по цене {fill_price}")
                else:
                    txt = (f"{pos_color(side)} Trader: {sym} Увеличение позиции {side} "
                           f"({old_amt} --> {new_amt}) по цене {fill_price}")

                tg_a(txt)
                pg_upsert_position(
                    "positions", sym, side,
                    new_amt, fill_price,  # entry_price меняем на fill_price, упрощённо
                    old_rpnl,  # PnL пока без изменений
                    "binance",
                    False
                )

                # Зеркалирование
                if MIRROR_ENABLED and self.client_b:
                    m_qty = fill_qty * MIRROR_COEFFICIENT
                    self.client_b.futures_create_order(
                        symbol=sym,
                        side="BUY" if side=="LONG" else "SELL",
                        type="MARKET",
                        quantity=m_qty
                    )
                    pg_upsert_position(
                        "mirror_positions", sym, side,
                        m_qty, fill_price, 0.0,
                        "mirror", False
                    )
                    tg_m(f"[Mirror]: {pos_color(side)} Trader: {sym} "
                         f"Открыта/увеличена {side} (by MARKET). "
                         f"Объём: {m_qty}, Цена: {fill_price}")

    def _diff_positions(self):
        """
        Сравнение фактических позиций на Binance и в БД — пока заглушка.
        """
        log.debug("_diff_positions called")
        pass

    def _sync_start(self):
        """
        При старте: показать реально открытые позиции + SL/TP;
        Пометить NEW LIMIT ордера как pending
        """
        log.debug("_sync_start called")
        try:
            positions = self.client_a.futures_position_information()
            for p in positions:
                amt = abs(float(p["positionAmt"]))
                if amt < 1e-12:
                    continue
                sym   = p["symbol"]
                side  = "LONG" if float(p["positionAmt"])>0 else "SHORT"
                price = float(p["entryPrice"])

                sl = tp = None
                open_orders = self.client_a.futures_get_open_orders(symbol=sym)
                for od in open_orders:
                    if od["type"] in CHILD_TYPES and od["status"]=="NEW":
                        trg = float(od.get("stopPrice") or od.get("price") or 0)
                        if trg:
                            if "STOP" in od["type"]:
                                sl = trg
                            else:
                                tp = trg

                txt = (f"{pos_color(side)} (start) Trader: {sym} "
                       f"Открыта {side} Объём: {amt}, Цена: {price}")
                if sl is not None:
                    txt += f", SL={sl}"
                if tp is not None:
                    txt += f", TP={tp}"
                tg_a(txt)

                pg_upsert_position("positions", sym, side, amt, price, 0.0, "binance", False)
        except Exception as e:
            log.error("_sync_start positions: %s", e)

        # pending LIMIT
        try:
            orders = self.client_a.futures_get_open_orders()
            for od in orders:
                if od["type"]=="LIMIT" and od["status"]=="NEW":
                    sym   = od["symbol"]
                    price = float(od["price"])
                    qty   = float(od["origQty"])
                    side  = "LONG" if od["side"]=="BUY" else "SHORT"

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

                    txt = (f"🔵 (start) Trader: {sym} Новый LIMIT "
                           f"{pos_color(side)} {side}. Объём: {qty} по цене {price}.")
                    if sl is not None:
                        txt += f" SL={sl}"
                    if tp is not None:
                        txt += f" TP={tp}"
                    tg_a(txt)

                    pg_upsert_position("positions", sym, side, qty, price, 0.0, "binance", True)
        except Exception as e:
            log.error("_sync_start limits: %s", e)

    def _hello(self):
        """
        Приветствие + балансы
        """
        log.debug("_hello called")
        bal_a = self._usdt(self.client_a)
        msg   = f"▶️  Бот запущен.\nОсновной аккаунт: {bal_a:.2f} USDT"
        if self.client_b:
            bal_b = self._usdt(self.client_b)
            msg  += f"\nЗеркальный аккаунт активен: {bal_b:.2f} USDT"
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