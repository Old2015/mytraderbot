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
    wipe_mirror, reset_pending,
    pg_upsert_order, pg_delete_order
)
from telegram_bot import tg_a, tg_m

log = logging.getLogger(__name__)

# Типы «дочерних» ордеров (STOP, TAKE_PROFIT…)
CHILD_TYPES = {
    "STOP","STOP_MARKET","STOP_LOSS","STOP_LOSS_LIMIT",
    "TAKE_PROFIT","TAKE_PROFIT_LIMIT","TAKE_PROFIT_MARKET"
}

def pos_color(side: str) -> str:
    return "🟢" if side == "LONG" else "🔴"

def child_color() -> str:
    return "🔵"

def decode_side(o: Dict[str,Any]) -> str:
    """
    Учитываем reduceOnly (R): если R=true + "BUY" -> закрываем SHORT,
    если R=false + "BUY" -> открываем/увеличиваем LONG, и т.д.
    """
    reduce_flag = bool(o.get("R", False))
    raw_side = o["S"]  # "BUY" / "SELL"
    if reduce_flag:
        if raw_side == "BUY":
            return "SHORT"
        else:
            return "LONG"
    else:
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
    Форматируем число, убирая хвостовые нули.
    """
    s = f"{x:.{digits}f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s

class AlexBot:
    """
    Бот, где мы в таблице 'positions' храним реальные позиции,
    а в таблице 'orders' — лимиты/стопы (pending).
    """

    def __init__(self):
        log.debug("AlexBot.__init__ called")

        self.client_a = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        self.client_b = (
            Client(MIRROR_B_API_KEY, MIRROR_B_API_SECRET)
            if MIRROR_ENABLED else None
        )

        # Словари точностей
        self.lot_size_map   = {}
        self.price_size_map = {}
        self._init_symbol_precisions()

        # Запуск WebSocket
        self.ws = ThreadedWebsocketManager(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET
        )
        self.ws.start()
        self.ws.start_futures_user_socket(callback=self._ws_handler)

        # Сброс зеркальных позиций / pending
        wipe_mirror()
        reset_pending()

        # Синхронизация
        self._sync_start()
        self._hello()

    # -------- точность --------
    def _init_symbol_precisions(self):
        log.debug("_init_symbol_precisions called")
        try:
            info = self.client_a.futures_exchange_info()
            for s in info["symbols"]:
                sym_name = s["symbol"]
                lot_dec, price_dec = 4, 4
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        lot_dec = self._step_to_decimals(f["stepSize"])
                    elif f["filterType"] == "PRICE_FILTER":
                        price_dec = self._step_to_decimals(f["tickSize"])
                self.lot_size_map[sym_name]   = lot_dec
                self.price_size_map[sym_name] = price_dec

            log.info("_init_symbol_precisions: loaded %d pairs", len(info["symbols"]))
        except Exception as e:
            log.error("_init_symbol_precisions: %s", e)

    @staticmethod
    def _step_to_decimals(step_str: str) -> int:
        s = step_str.rstrip('0')
        if '.' not in s:
            return 0
        return len(s.split('.')[1])

    def _fmt_qty(self, symbol: str, qty: float) -> str:
        dec = self.lot_size_map.get(symbol, 4)
        val = f"{qty:.{dec}f}"
        return val.rstrip('0').rstrip('.') if '.' in val else val

    def _fmt_price(self, symbol: str, price: float) -> str:
        dec = self.price_size_map.get(symbol, 4)
        val = f"{price:.{dec}f}"
        return val.rstrip('0').rstrip('.') if '.' in val else val
    # -----------------------------

    def _hello(self):
        """
        Приветственное сообщение
        """
        bal_a = self._usdt(self.client_a)
        msg = f"▶️  Бот запущен.\nОсновной аккаунт: {_fmt_float(bal_a)} USDT"
        if self.client_b and MIRROR_ENABLED:
            bal_b = self._usdt(self.client_b)
            msg += f"\nЗеркальный аккаунт активен: {_fmt_float(bal_b)} USDT"

        log.info(msg)
        tg_m(msg)

    def _usdt(self, client: Client) -> float:
        try:
            bals = client.futures_account_balance()
            for b in bals:
                if b["asset"] == "USDT":
                    return float(b["balance"])
        except Exception as e:
            log.error("_usdt: %s", e)
        return 0.0

    def _sync_start(self):
        """
        При старте:
          1) Реальные позиции => positions
          2) Открытые ордера => orders
          3) Удаляем лишнее
        """
        log.debug("_sync_start called")
        try:
            # 1) Позы
            pos_info = self.client_a.futures_position_information()
            real_positions = set()

            for p in pos_info:
                amt = float(p["positionAmt"])
                if abs(amt) < 1e-12:
                    continue
                sym  = p["symbol"]
                side = "LONG" if amt>0 else "SHORT"
                prc  = float(p["entryPrice"])
                vol  = abs(amt)
                real_positions.add((sym, side))

                txt = (f"{pos_color(side)} (start) Trader: {sym} "
                       f"Открыта {side}, Объём={self._fmt_qty(sym, vol)}, "
                       f"Цена={self._fmt_price(sym, prc)}")
                tg_a(txt)

                pg_upsert_position("positions", sym, side, vol, prc, 0.0, "binance", False)

            # 2) Ордеры
            all_orders = self.client_a.futures_get_open_orders()
            real_ords  = set()
            from db import pg_upsert_order

            for od in all_orders:
                # status=NEW => значит активен
                if od["status"]=="NEW":
                    otype = od["type"]        # "LIMIT","STOP_MARKET","TAKE_PROFIT" etc.
                    raw_side = od["side"]     # "BUY"/"SELL"
                    side = "LONG" if raw_side=="BUY" else "SHORT"
                    oid  = int(od["orderId"])
                    # Qty + Price:
                    # Для LIMIT => price=od["price"], qty=od["origQty"]
                    # Для STOP,TAKE => price=od["stopPrice"] (или fallback?), qty=od["origQty"]
                    # Часто BINANCE вернёт 0.0,0.0 для closePosition => handle gracefully

                    # Попробуем универсальный подход:
                    orig_qty = float(od.get("origQty",0))
                    stop_p   = float(od.get("stopPrice",0))
                    limit_p  = float(od.get("price",0))

                    # Логика: если type in CHILD_TYPES => используем stopPrice (если >0)
                    if otype in CHILD_TYPES:
                        price = stop_p if stop_p>1e-12 else limit_p
                    else:
                        price = limit_p

                    # msg
                    # Если origQty=0, может быть closePosition
                    if orig_qty<1e-12 and od.get("closePosition","false")=="true":
                        qty_str = "closePosition"
                    else:
                        qty_str = self._fmt_qty(sym, orig_qty)

                    # upsert в orders
                    pg_upsert_order(sym, side, oid, orig_qty, price, "NEW")
                    real_ords.add((sym, side, oid))

                    # Сообщение
                    txt = (f"🔵(start) {sym} {pos_color(side)} {side} {otype}, "
                           f"Qty={qty_str}, Price={self._fmt_price(sym, price)}, orderId={oid}")
                    tg_a(txt)

            # 3) Удаляем лишние
            #   a) positions
            with pg_conn() as conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol, position_side
                      FROM public.positions
                     WHERE exchange='binance';
                """)
                rows = cur.fetchall()
                for (db_sym, db_side) in rows:
                    if (db_sym, db_side) not in real_positions:
                        log.info("Removing old position: %s, %s", db_sym, db_side)
                        pg_delete_position("positions", db_sym, db_side)

            #   b) orders
            from db import pg_delete_order
            with pg_conn() as conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol, position_side, order_id
                      FROM public.orders
                """)
                rows = cur.fetchall()
                for (db_sym, db_side, db_oid) in rows:
                    if (db_sym, db_side, db_oid) not in real_ords:
                        log.info("Removing old order from DB: %s %s %s", db_sym, db_side, db_oid)
                        pg_delete_order(db_sym, db_side, db_oid)

        except Exception as e:
            log.error("_sync_start: %s", e)

    def _ws_handler(self, msg: Dict[str,Any]):
        pg_raw(msg)
        log.debug("[WS] %s", msg)
        if msg.get("e") == "ORDER_TRADE_UPDATE":
            self._on_order(msg["o"])

    def _on_order(self, o: Dict[str,Any]):
        """
        При любом событии:
         - CANCEL LIMIT => pg_delete_order,
         - NEW LIMIT => pg_upsert_order,
         - FILLED => remove from orders + update positions,
         - STOP/TAKE FILLED => remove from orders + update positions,
         - зеркалирование ...
        """
        sym    = o["s"]
        otype  = o["ot"]
        status = o["X"]
        fill_price = float(o.get("ap", 0))
        fill_qty   = float(o.get("l", 0))
        reduce_flag= bool(o.get("R", False))
        side       = decode_side(o)
        partial_pnl= float(o.get("rp", 0.0))
        rtxt       = reason_text(otype)
        order_id   = int(o.get("i", 0))

        # import for orders
        from db import pg_upsert_order, pg_delete_order

        # CANCEL LIMIT
        if otype=="LIMIT" and status=="CANCELED":
            pg_delete_order(sym, side, order_id)
            price = float(o.get("p",0))
            qty   = float(o.get("q",0))
            tg_a(f"🔵 Trader: {sym} LIMIT отменен (orderId={order_id}), qty={qty}, price={price}")
            return

        # NEW LIMIT
        if otype=="LIMIT" and status=="NEW":
            qty   = float(o.get("q",0))
            price = float(o.get("p",0))
            pg_upsert_order(sym, side, order_id, qty, price, "NEW")
            tg_a(f"🔵 Trader: {sym} Новый LIMIT {side} (orderId={order_id}), qty={qty}, price={price}")
            return

        # NEW STOP/TAKE
        if otype in CHILD_TYPES and status=="NEW":
            stp = float(o.get("sp") or o.get("p") or 0)
            q   = float(o.get("q",0))
            pg_upsert_order(sym, side, order_id, q, stp, "NEW")
            kind = "STOP" if "STOP" in otype else "TAKE"
            tg_a(f"{child_color()} Trader: {sym} {kind} установлен (orderId={order_id}), qty={q}, price={stp}")
            return

        # FILLED (MARKET/LIMIT/STOP/TAKE)
        if status=="FILLED":
            # Удаляем из orders (если LIMIT / STOP / TAKE)
            if otype=="LIMIT" or otype in CHILD_TYPES:
                pg_delete_order(sym, side, order_id)

            if fill_qty<1e-12:
                return

            # Если STOP => "STOP активирован" etc.
            if otype in CHILD_TYPES:
                kind = "STOP" if "STOP" in otype else "TAKE"
                stp  = float(o.get("sp",0))
                tg_a(f"{child_color()} Trader: {sym} {kind} активирован (sp={stp}, fill_price={fill_price})")

            old_amt, old_entry, old_rpnl = pg_get_position("positions", sym, side) or (0.0,0.0,0.0)
            new_rpnl = old_rpnl + partial_pnl

            if reduce_flag:
                # Закрытие
                new_amt = old_amt - fill_qty
                ratio   = (fill_qty/old_amt)*100 if old_amt>1e-12 else 100
                if ratio>100: ratio=100
                if new_amt<=1e-8:
                    # Полное
                    tg_a(f"{pos_color(side)} Trader: {sym} полное закрытие {side} "
                         f"({int(ratio)}%, {old_amt}->{0}), price={fill_price}, PNL={_fmt_float(new_rpnl)}")
                    pg_delete_position("positions", sym, side)
                else:
                    tg_a(f"{pos_color(side)} Trader: {sym} частичное закрытие {side} "
                         f"({int(ratio)}%, {old_amt}->{new_amt}), price={fill_price}, PNL={_fmt_float(new_rpnl)}")
                    pg_upsert_position("positions", sym, side, new_amt, old_entry, new_rpnl, "binance", False)

                if MIRROR_ENABLED:
                    self._mirror_reduce(sym, side, fill_qty, fill_price, partial_pnl)
            else:
                # Открытие / увеличение
                new_amt = old_amt + fill_qty
                if old_amt<1e-12:
                    # Новая позиция
                    tg_a(f"{pos_color(side)} Trader: {sym} Открыта позиция {side} {rtxt}, "
                         f"qty={fill_qty}, price={fill_price}")
                else:
                    ratio= (fill_qty/old_amt)*100 if old_amt>1e-12 else 100
                    if ratio>100: ratio=100
                    tg_a(f"{pos_color(side)} Trader: {sym} Увеличение позиции {side} "
                         f"({int(ratio)}%, {old_amt}->{new_amt}), {rtxt}, price={fill_price}")

                pg_upsert_position("positions", sym, side, new_amt, fill_price, new_rpnl, "binance", False)
                if MIRROR_ENABLED:
                    self._mirror_increase(sym, side, fill_qty, fill_price, rtxt)

    def _mirror_reduce(self, sym: str, side: str, fill_qty: float, fill_price: float, partial_pnl: float):
        """
        Аналогично старому коду, reduceOnly MARKET
        """
        old_m_amt, old_m_entry, old_m_rpnl = pg_get_position("mirror_positions", sym, side) or (0.0,0.0,0.0)
        dec_qty= fill_qty * MIRROR_COEFFICIENT
        new_m_pnl= old_m_rpnl + partial_pnl*MIRROR_COEFFICIENT
        new_m_amt= old_m_amt - dec_qty

        ratio = (dec_qty/old_m_amt)*100 if old_m_amt>1e-12 else 100
        if ratio>100: ratio=100

        side_binance = "BUY" if side=="SHORT" else "SELL"
        try:
            self.client_b.futures_create_order(
                symbol=sym,
                side=side_binance,
                type="MARKET",
                quantity=dec_qty,
                reduceOnly=True
            )
        except Exception as e:
            log.error("_mirror_reduce: %s", e)

        if new_m_amt<=1e-8:
            pg_delete_position("mirror_positions", sym, side)
            tg_m(f"[Mirror]: {pos_color(side)} Trader: {sym} полное закрытие {side}, ratio={int(ratio)}%, price={fill_price}")
        else:
            pg_upsert_position("mirror_positions", sym, side, new_m_amt, old_m_entry, new_m_pnl, "mirror", False)
            tg_m(f"[Mirror]: {pos_color(side)} Trader: {sym} частичное закрытие {side}, ratio={int(ratio)}%, price={fill_price}")

    def _mirror_increase(self, sym: str, side: str, fill_qty: float, fill_price: float, rtxt: str):
        """
        Открытие / увеличение на зеркальном
        """
        old_m_amt, old_m_entry, old_m_rpnl = pg_get_position("mirror_positions", sym, side) or (0.0,0.0,0.0)
        inc_qty= fill_qty*MIRROR_COEFFICIENT
        new_m_amt= old_m_amt+inc_qty

        side_binance= "BUY" if side=="LONG" else "SELL"
        try:
            self.client_b.futures_create_order(
                symbol=sym,
                side=side_binance,
                type="MARKET",
                quantity=inc_qty
            )
        except Exception as e:
            log.error("_mirror_increase: %s", e)

        pg_upsert_position("mirror_positions", sym, side, new_m_amt, fill_price, old_m_rpnl, "mirror", False)
        if old_m_amt<1e-12:
            tg_m(f"[Mirror]: {pos_color(side)} Trader: {sym} Открыта {side} {rtxt}, qty={inc_qty}, price={fill_price}")
        else:
            ratio= (inc_qty/old_m_amt)*100 if old_m_amt>1e-12 else 100
            if ratio>100: ratio=100
            tg_m(f"[Mirror]: {pos_color(side)} Trader: {sym} Увеличение {side}, ratio={int(ratio)}%, price={fill_price}")

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