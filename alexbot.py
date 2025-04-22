import time
import logging
import json
from typing import Dict, Any

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
        # NEW: логируем запуск конструктора
        log.debug("AlexBot.__init__ called")

        self.client_a = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        self.client_b = (
            Client(MIRROR_B_API_KEY, MIRROR_B_API_SECRET)
            if MIRROR_ENABLED else None
        )

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

    def _sync_start(self):
        # NEW: логируем вход
        log.debug("_sync_start: begin")
        """
        1) Выводим реально открытые позиции (и SL/TP).
        2) Помечаем как pending все NEW LIMIT-ордера.
        """
        # 1) открытые позиции
        try:
            positions = self.client_a.futures_position_information()
            log.debug("_sync_start: got %d positions from binance", len(positions))
            for p in positions:
                amt = abs(float(p["positionAmt"]))
                if amt < 1e-12:
                    continue
                sym   = p["symbol"]
                side  = "LONG" if float(p["positionAmt"])>0 else "SHORT"
                price = float(p["entryPrice"])

                # Найдём SL/TP
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

                txt = (f"{pos_color(side)} (start) Trader: {sym} "
                       f"Открыта {side} Объём: {amt}, Цена: {price}")
                if sl is not None:
                    txt += f", SL={sl}"
                if tp is not None:
                    txt += f", TP={tp}"
                tg_a(txt)

                pg_upsert_position(
                    "positions", sym, side, amt, price,
                    0.0, "binance", False
                )
        except Exception as e:
            log.error("_sync_start positions: %s", e)

        # 2) pending LIMIT-ордера
        try:
            orders = self.client_a.futures_get_open_orders()
            log.debug("_sync_start: got %d open_orders", len(orders))
            for od in orders:
                if od["type"]=="LIMIT" and od["status"]=="NEW":
                    sym   = od["symbol"]
                    price = float(od["price"])
                    qty   = float(od["origQty"])
                    side  = "LONG" if od["side"]=="BUY" else "SHORT"
                    sl = tp = None
                    for ch in orders:
                        if (
                            ch["symbol"]==sym and ch["status"]=="NEW"
                            and ch["orderId"]!=od["orderId"]
                            and ch["type"] in CHILD_TYPES
                        ):
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
                    pg_upsert_position(
                        "positions", sym, side, qty, price,
                        0.0, "binance", True
                    )
        except Exception as e:
            log.error("_sync_start limits: %s", e)

        # NEW: логируем выход
        log.debug("_sync_start: end")

    def _hello(self):
        # NEW
        log.debug("_hello: begin")
        bal_a = self._usdt(self.client_a)
        msg   = f"▶️  Бот запущен.\nОсновной аккаунт: {bal_a:.2f} USDT"
        if self.client_b:
            bal_b = self._usdt(self.client_b)
            msg  += f"\nЗеркальный аккаунт активен: {bal_b:.2f} USDT"
        tg_m(msg)
        log.debug("_hello: end")

    def _ws_handler(self, msg: Dict[str,Any]):
        # NEW
        log.debug("_ws_handler called with msg=%s", msg)
        """
        Callback на каждое WS-сообщение:
        - Сохраняем сырое msg в БД (pg_raw)
        - log.info("[WS] ...")
        - Если ORDER_TRADE_UPDATE -> _on_order + _diff_positions
        """
        pg_raw(msg)
        log.info("[WS] %s", msg)

        if msg.get("e")=="ORDER_TRADE_UPDATE":
            o = msg["o"]
            self._on_order(o)
            self._diff_positions()

    def _on_order(self, o: Dict[str,Any]):
        # NEW
        log.debug("_on_order called with o=%s", o)
        sym, otype, status = o["s"], o["ot"], o["X"]
        side  = "LONG" if o["S"]=="BUY" else "SHORT"

        # NEW LIMIT
        if otype=="LIMIT" and status=="NEW":
            price = float(o["p"])
            qty   = float(o["q"])
            sl, tp = self._find_sl_tp(sym)
            txt = (f"🔵 Trader: {sym} Новый LIMIT {pos_color(side)} {side}. "
                   f"Объём: {qty} по цене {price}.")
            if sl is not None:
                txt += f" SL={sl}"
            if tp is not None:
                txt += f" TP={tp}"
            tg_a(txt)
            pg_upsert_position("positions", sym, side, qty, price, 0.0, "binance", True)
            return

        # CANCELED LIMIT
        if otype=="LIMIT" and status=="CANCELED":
            price = float(o.get("p", 0))
            qty   = float(o.get("q", 0))
            txt   = (f"🔵 Trader: {sym} LIMIT {price}. "
                     f"{pos_color(side)} {side}, Объём: {qty} отменён")
            tg_a(txt)
            pg_upsert_position("positions", sym, side, 0.0, 0.0, 0.0, "binance", False)
            return

        # NEW child STOP/TAKE
        if otype in CHILD_TYPES and status=="NEW":
            trg = float(o.get("sp") or o.get("p") or 0)
            if trg:
                kind = "STOP" if "STOP" in otype else "TAKE"
                tg_a(f"{child_color()} Trader: {sym} {kind} установлен на цену {trg}")
            return

        # FILLED (MARKET/LIMIT)
        if status=="FILLED":
            reduce_flag = bool(o.get("R", False))
            fill_price  = float(o.get("ap", 0))
            fill_qty    = float(o.get("l", 0))

            if reduce_flag:
                txt = (f"{pos_color(side)} Trader: {sym} (reduce-only) "
                       f"Закрыто {fill_qty} по цене {fill_price}")
                tg_a(txt)
            else:
                txt = (f"{pos_color(side)} Trader: {sym} Открыта/увеличена {side} "
                       f"на {fill_qty} по цене {fill_price}")
                tg_a(txt)
                pg_upsert_position(
                    "positions", sym, side, fill_qty, fill_price,
                    0.0, "binance", False
                )

                # Зеркало
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
                         f"Открыта {side} (by MARKET). Объём: {m_qty}, Цена: {fill_price}")

    def _diff_positions(self):
        # NEW
        log.debug("_diff_positions called")
        """
        Сравнение фактических позиций на Binance и в БД — пока заглушка
        """
        pass

    def _find_sl_tp(self, symbol:str):
        # NEW
        log.debug("_find_sl_tp: symbol=%s", symbol)
        sl = tp = None
        try:
            open_orders = self.client_a.futures_get_open_orders(symbol=symbol)
            for od in open_orders:
                if od["type"] in CHILD_TYPES and od["status"]=="NEW":
                    trg = float(od.get("stopPrice") or od.get("price") or 0)
                    if trg:
                        if "STOP" in od["type"]:
                            sl = trg
                        else:
                            tp = trg
        except Exception as e:
            log.error("_find_sl_tp: %s", e)
        return sl, tp

    @staticmethod
    def _usdt(client: Client) -> float:
        # NEW
        log.debug("_usdt called")
        try:
            balances = client.futures_account_balance()
            for b in balances:
                if b["asset"] == "USDT":
                    return float(b["balance"])
        except Exception as e:
            log.error("_usdt: %s", e)
        return 0.0

    def run(self):
        # NEW
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