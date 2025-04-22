import logging
import time
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
    pg_raw, pg_upsert_position, wipe_mirror, reset_pending
)
from telegram_bot import tg_a, tg_m

log = logging.getLogger(__name__)

# Какие типы ордеров считаем "дочерними" для SL/TP?
CHILD_TYPES = {
    "STOP", "STOP_MARKET", "STOP_LOSS", "STOP_LOSS_LIMIT",
    "TAKE_PROFIT", "TAKE_PROFIT_LIMIT", "TAKE_PROFIT_MARKET"
}

def pos_color(side: str) -> str:
    return "🟢" if side == "LONG" else "🔴"

def child_color() -> str:
    return "🔵"

class AlexBot:
    def __init__(self):
        # Основной клиент
        self.client_a = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

        # Зеркальный клиент (если включено)
        self.client_b = None
        if MIRROR_ENABLED:
            self.client_b = Client(MIRROR_B_API_KEY, MIRROR_B_API_SECRET)

        # Запуск WebSocket
        self.ws = ThreadedWebsocketManager(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET
        )
        self.ws.start()
        self.ws.start_futures_user_socket(callback=self._ws_handler)

        # Инициализация
        wipe_mirror()    # очищаем mirror_positions
        reset_pending()  # сбрасываем pending
        self._sync_start()  # показать открытые позиции и LIMIT
        self._hello()       # приветствие и балансы

    def _sync_start(self):
        """
        1) Выводим реально открытые позиции (и SL/TP).
        2) Помечаем как pending все NEW LIMIT‑ордера.
        """
        # 1) открытые позиции
        try:
            positions = self.client_a.futures_position_information()
            for p in positions:
                amt = float(p["positionAmt"])
                if abs(amt) < 1e-12:
                    continue
                sym = p["symbol"]
                side = "LONG" if amt > 0 else "SHORT"
                price = float(p["entryPrice"])

                # Найдём SL/TP (дочерние ордера)
                open_orders = self.client_a.futures_get_open_orders(symbol=sym)
                sl, tp = None, None
                for od in open_orders:
                    if od["type"] in CHILD_TYPES and od["status"]=="NEW":
                        trg = float(od.get("stopPrice") or od.get("price") or 0)
                        if trg:
                            if "STOP" in od["type"]:
                                sl = trg
                            else:
                                tp = trg

                # Telegram-сообщение
                txt = f"{pos_color(side)} (start) Trader: {sym} Открыта {side} Объём: {abs(amt)}, Цена: {price}"
                if sl is not None:
                    txt += f", SL={sl}"
                if tp is not None:
                    txt += f", TP={tp}"
                tg_a(txt)

                # Сохраняем в БД
                pg_upsert_position(
                    "positions", sym, side, abs(amt), price,
                    pnl=0.0, exchange="binance", pending=False
                )
        except Exception as e:
            log.error("_sync_start positions: %s", e)

        # 2) pending LIMIT‑ордера
        try:
            orders = self.client_a.futures_get_open_orders()
            for od in orders:
                if od["type"]=="LIMIT" and od["status"]=="NEW":
                    sym   = od["symbol"]
                    price = float(od["price"])
                    qty   = float(od["origQty"])
                    side  = "LONG" if od["side"]=="BUY" else "SHORT"

                    # Ищем SL/TP, привязанные к этому лимиту
                    # (возможно, это условно — потому что Binance не всегда явно связывает)
                    sl, tp = None, None
                    for ch in orders:
                        if (ch["symbol"] == sym and ch["status"]=="NEW"
                                and ch["orderId"] != od["orderId"]
                                and ch["type"] in CHILD_TYPES):
                            trg = float(ch.get("stopPrice") or ch.get("price") or 0)
                            if trg:
                                if "STOP" in ch["type"]:
                                    sl = trg
                                else:
                                    tp = trg

                    txt = (f"🔵 (start) Trader: {sym} Новый LIMIT {pos_color(side)} {side}. "
                           f"Объём: {qty} по цене {price}.")
                    if sl is not None:
                        txt += f" SL={sl}"
                    if tp is not None:
                        txt += f" TP={tp}"
                    tg_a(txt)

                    pg_upsert_position(
                        "positions", sym, side, qty, price,
                        pnl=0.0, exchange="binance", pending=True
                    )
        except Exception as e:
            log.error("_sync_start limits: %s", e)

    def _hello(self):
        """
        Приветственное сообщение и вывод балансов
        """
        bal_a = self._usdt(self.client_a)
        msg = f"▶️ Бот запущен.\nОсновной аккаунт: {bal_a:.2f} USDT"

        if self.client_b:
            bal_b = self._usdt(self.client_b)
            msg += f"\nЗеркальный аккаунт активен: {bal_b:.2f} USDT"

        tg_m(msg)

    def _ws_handler(self, msg: Dict[str, Any]):
        """
        Callback для каждого WS-сообщения:
         - Сохраняем "сырое" в БД (pg_raw)
         - Логируем
         - Если ORDER_TRADE_UPDATE → обрабатываем _on_order
           + _diff_positions() (опционально)
        """
        pg_raw(msg)
        log.info("[WS] %s", msg)
        if msg.get("e") == "ORDER_TRADE_UPDATE":
            o = msg["o"]
            self._on_order(o)
            self._diff_positions()

    def _on_order(self, o: Dict[str, Any]):
        """
        Обработка событий ордеров:
         - NEW LIMIT
         - CANCELED LIMIT
         - NEW child STOP/TAKE
         - FILLED MARKET/LIMIT (reduceOnly или нет)
        """
        sym    = o["s"]
        otype  = o["ot"]
        status = o["X"]
        side   = "LONG" if o["S"]=="BUY" else "SHORT"

        # --- NEW LIMIT ---
        if otype=="LIMIT" and status=="NEW":
            price = float(o["p"])        # price
            qty   = float(o["q"])        # origQty
            sl, tp = self._find_sl_tp(sym)
            txt = (f"🔵 Trader: {sym} Новый LIMIT {pos_color(side)} {side}. "
                   f"Объём: {qty} по цене {price}.")
            if sl is not None:
                txt += f" SL={sl}"
            if tp is not None:
                txt += f" TP={tp}"
            tg_a(txt)
            pg_upsert_position("positions", sym, side, qty, price,
                               pnl=0.0, exchange="binance", pending=True)
            return

        # --- CANCELED LIMIT ---
        if otype=="LIMIT" and status=="CANCELED":
            price = float(o.get("p", 0))
            qty   = float(o.get("q", 0))
            txt = (f"🔵 Trader: {sym} LIMIT {price}. "
                   f"{pos_color(side)} {side}, Объём: {qty} отменён")
            tg_a(txt)
            # Считаем, что это pending = False, объём = 0
            pg_upsert_position("positions", sym, side, 0.0, 0.0,
                               pnl=0.0, exchange="binance", pending=False)
            return

        # --- NEW child STOP/TAKE ---
        if otype in CHILD_TYPES and status=="NEW":
            trg = float(o.get("sp") or o.get("p") or 0)
            if trg:
                kind = "STOP" if "STOP" in otype else "TAKE"
                tg_a(f"{child_color()} Trader: {sym} {kind} установлен на цену {trg}")
            return

        # --- FILLED (MARKET/LIMIT) ---
        if status == "FILLED":
            reduce_flag = bool(o.get("R", False))
            fill_price  = float(o.get("ap", 0))  # avgPrice
            fill_qty    = float(o.get("l", 0))   # исполнено в последней сделке

            if reduce_flag:
                # Ордер с флагом reduceOnly => закрытие (или частичное закрытие)
                txt = (f"{pos_color(side)} Trader: {sym} (reduce-only) "
                       f"Закрыто {fill_qty} по цене {fill_price}")
                tg_a(txt)
                # Тут можно либо сразу вызвать _diff_positions(), 
                # либо уменьшить объём в нашей таблице "positions" 
                # на fill_qty (с учётом текущего значения).
                # Но проще пусть _diff_positions() сама всё подкорректирует:
            else:
                # Открытие/увеличение позиции
                txt = (f"{pos_color(side)} Trader: {sym} Открыта/увеличена {side} "
                       f"на {fill_qty} по цене {fill_price}")
                tg_a(txt)
                # Запись в БД (пока простая, без учёта частичных исполнений):
                # Окончательный объём позиции тоже лучше возложить на _diff_positions().
                # Но можно тут сделать upsert с "qty" = fill_qty 
                # (или суммировать, если выполнялось частями).
                # Упростим до:
                pg_upsert_position("positions", sym, side, fill_qty, fill_price,
                                   pnl=0.0, exchange="binance", pending=False)

                # Зеркалирование
                if MIRROR_ENABLED and self.client_b:
                    m_qty = fill_qty * MIRROR_COEFFICIENT
                    resp = self.client_b.futures_create_order(
                        symbol=sym,
                        side="BUY" if side=="LONG" else "SELL",
                        type="MARKET",
                        quantity=m_qty
                    )
                    pg_upsert_position(
                        "mirror_positions", sym, side,
                        m_qty, fill_price, 0.0,
                        exchange="mirror", pending=False
                    )
                    tg_m(f"[Mirror]: {pos_color(side)} Trader: {sym} "
                         f"Открыта {side} (by MARKET). Объём: {m_qty}, Цена: {fill_price}")

    def _find_sl_tp(self, symbol: str):
        """
        Вспомогательный метод, чтобы найти SL/TP для символа.
        Возвращает (sl_price, tp_price) или (None, None).
        """
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

    def _diff_positions(self):
        """
        Метод для сравнения фактических позиций на Binance и в БД.
        При различиях обновляем, шлём Telegram.
        Сейчас заглушка, логика на будущее.
        """
        pass

    @staticmethod
    def _usdt(client: Client) -> float:
        """
        Возвращает баланс USDT для данного фьючерс-клиента.
        """
        try:
            balances = client.futures_account_balance()
            for b in balances:
                if b["asset"] == "USDT":
                    return float(b["balance"])
        except Exception as e:
            log.error("_usdt: %s", e)
        return 0.0

    def run(self):
        """
        Основной цикл — просто держим программу в работе, пока не прервём Ctrl+C.
        """
        try:
            log.info("[Main] bot running ... Ctrl+C to stop")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            tg_m("⏹️ Бот остановлен пользователем")
        finally:
            self.ws.stop()
            log.info("[Main] bye.")
