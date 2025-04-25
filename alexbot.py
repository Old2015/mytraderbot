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

CHILD_TYPES = {
    "STOP","STOP_MARKET","STOP_LOSS","STOP_LOSS_LIMIT",
    "TAKE_PROFIT","TAKE_PROFIT_LIMIT","TAKE_PROFIT_MARKET"
}

def pos_color(side: str) -> str:
    """
    Возвращаем 🟢 для ЛОНГ, 🔴 для ШОРТ
    """
    return "🟢" if side=="LONG" else "🔴"

def side_text(side: str) -> str:
    """
    Возвращаем «ЛОНГ» или «ШОРТ»
    """
    return "ЛОНГ" if side=="LONG" else "ШОРТ"

def child_color() -> str:
    """
    Для STOP/TAKE используем 🔵
    """
    return "🔵"

def reason_text(otype: str) -> str:
    """
    Расширяем для MARKET, LIMIT, STOP, TAKE_PROFIT etc
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

def _fmt_float(x: float, digits:int=4) -> str:
    """
    Форматирование числа x с digits знаками, убираем лишние нули
    """
    s = f"{x:.{digits}f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s

def decode_side_binance(raw_side:str, reduce_f:bool, closepos:bool=False) -> str:
    """
    Если reduceOnly=true / closePosition=true, "BUY" => SHORT (закрываем SHORT? нет, 
    actually if raw_side=BUY => closing SHORT, so side=SHORT?), 
    or if raw_side=SELL => closing LONG => side=LONG.
    Иначе — обычная логика BUY => LONG, SELL => SHORT.
    """
    if reduce_f or closepos:
        # Закрытие
        if raw_side=="BUY":
            return "SHORT"
        else:
            return "LONG"
    else:
        # Открытие
        if raw_side=="BUY":
            return "LONG"
        else:
            return "SHORT"

class AlexBot:
    """
    Бот, где:
     - positions: храним реальную позицию,
     - orders: храним активные (лимит/стоп) ордера,
     При старте: _sync_start => 
       1) positions (futures_position_information),
       2) orders (futures_get_open_orders),
       3) чистим лишнее,
     STOP/TAKE выводим как 🔵, 
     LIMIT/позиции => 🟢(ЛОНГ)/🔴(ШОРТ).
    """

    def __init__(self):
        log.debug("AlexBot.__init__ called")

        # Клиент для основного аккаунта
        self.client_a = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        # Клиент зеркального (если включено)
        self.client_b = (
            Client(MIRROR_B_API_KEY, MIRROR_B_API_SECRET)
            if MIRROR_ENABLED else None
        )

        self.lot_size_map = {}
        self.price_size_map= {}
        self._init_symbol_precisions()

        # WebSocket
        self.ws = ThreadedWebsocketManager(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET
        )
        self.ws.start()
        self.ws.start_futures_user_socket(callback=self._ws_handler)

        # Чистим mirror, сбрасываем pending
        wipe_mirror()
        reset_pending()

        # Синхронизация + приветствие
        self._sync_start()
        self._hello()

    # ------ точность ------
    def _init_symbol_precisions(self):
        log.debug("_init_symbol_precisions called")
        try:
            info = self.client_a.futures_exchange_info()
            for s in info["symbols"]:
                sym_name = s["symbol"]
                lot_dec = 4
                price_dec=4
                for f in s["filters"]:
                    if f["filterType"]=="LOT_SIZE":
                        lot_dec = self._step_to_decimals(f["stepSize"])
                    elif f["filterType"]=="PRICE_FILTER":
                        price_dec= self._step_to_decimals(f["tickSize"])
                self.lot_size_map[sym_name]= lot_dec
                self.price_size_map[sym_name]=price_dec
            log.info("_init_symbol_precisions: loaded %d", len(info["symbols"]))
        except Exception as e:
            log.error("_init_symbol_precisions: %s", e)

    @staticmethod
    def _step_to_decimals(step_str:str)->int:
        s = step_str.rstrip('0')
        if '.' not in s:
            return 0
        return len(s.split('.')[1])

    def _fmt_qty(self, sym:str, qty:float)->str:
        dec = self.lot_size_map.get(sym,4)
        val = f"{qty:.{dec}f}"
        return val.rstrip('0').rstrip('.') if '.' in val else val

    def _fmt_price(self, sym:str, price:float)->str:
        dec = self.price_size_map.get(sym,4)
        val = f"{price:.{dec}f}"
        return val.rstrip('0').rstrip('.') if '.' in val else val
    # -----------------------

    def _hello(self):
        bal_main = self._usdt(self.client_a)
        msg = f"▶️  Бот запущен.\nОсновной аккаунт: {_fmt_float(bal_main)} USDT"
        if self.client_b and MIRROR_ENABLED:
            bal_m = self._usdt(self.client_b)
            msg += f"\nЗеркальный аккаунт активен: {_fmt_float(bal_m)} USDT"
        log.info(msg)
        tg_m(msg)

    def _usdt(self, cl:Client)->float:
        try:
            bals = cl.futures_account_balance()
            for b in bals:
                if b["asset"]=="USDT":
                    return float(b["balance"])
        except Exception as e:
            log.error("_usdt: %s", e)
        return 0.0

    def _sync_start(self):
        """
        1) Смотрим реальные позиции => positions
        2) Смотрим openOrders => orders
        3) Чистим лишнее
        """
        log.debug("_sync_start called")
        try:
            # 1) Реальные позиции
            pos_info = self.client_a.futures_position_information()
            real_positions = set()
            for p in pos_info:
                amt = float(p["positionAmt"])
                if abs(amt)<1e-12:
                    continue
                sym  = p["symbol"]
                side = "LONG" if amt>0 else "SHORT"
                price= float(p["entryPrice"])
                vol  = abs(amt)
                real_positions.add((sym, side))

                txt = (f"{pos_color(side)} (start) Trader: {sym} "
                       f"Открыта {side_text(side)}, Объём={self._fmt_qty(sym, vol)}, "
                       f"Цена={self._fmt_price(sym, price)}")
                tg_a(txt)
                pg_upsert_position("positions", sym, side, vol, price, 0.0, "binance", False)

            # 2) Активные ордера => orders
            all_orders = self.client_a.futures_get_open_orders()
            real_orders= set()

            for od in all_orders:
                if od["status"]!="NEW":
                    continue
                # Определяем side = ЛОНГ/ШОРТ с учётом reduceOnly + closePosition
                raw_side = od["side"]  # "BUY"/"SELL"
                reduce_f = bool(od.get("reduceOnly",False))
                closepos = (od.get("closePosition","false")=="true")
                side = decode_side_binance(raw_side, reduce_f, closepos)

                otype = od["type"]  # "LIMIT", "STOP_MARKET", ...
                oid   = int(od["orderId"])
                sym   = od["symbol"]

                orig_qty   = float(od.get("origQty",0))
                stop_price = float(od.get("stopPrice",0))
                limit_price= float(od.get("price",0))

                # основная цена
                if otype in CHILD_TYPES:
                    # STOP/TAKE
                    main_price = stop_price if stop_price>1e-12 else limit_price
                else:
                    main_price = limit_price

                # qty_str
                if closepos and orig_qty<1e-12:
                    qty_str = "closePosition"
                else:
                    qty_str = self._fmt_qty(sym, orig_qty)

                # Сохраняем в orders
                pg_upsert_order(sym, side, oid, orig_qty, main_price, "NEW")
                real_orders.add((sym, side, oid))

                # Формируем сообщение
                if otype in CHILD_TYPES:
                    # STOP/TAKE => 🔵
                    kind = "STOP" if "STOP" in otype else "TAKE"
                    txt = (
                        f"{child_color()} (start) Trader: {sym} "
                        f"{side_text(side)} {kind} установлен на цену {self._fmt_price(sym, main_price)}"
                    )
                elif otype=="LIMIT":
                    # Пример
                    txt = (f"{pos_color(side)} (start) Trader: {sym} "
                           f"{side_text(side)} LIMIT, Qty={qty_str}, Price={self._fmt_price(sym, main_price)}")
                else:
                    # fallback
                    txt = (f"{pos_color(side)} (start) Trader: {sym} {side_text(side)} {otype}, "
                           f"Qty={qty_str}, Price={self._fmt_price(sym, main_price)}")

                tg_a(txt)

            # 3) Удаляем лишнее
            # a) positions
            with pg_conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT symbol, position_side FROM public.positions WHERE exchange='binance'")
                rows = cur.fetchall()
                for (db_sym, db_side) in rows:
                    if (db_sym, db_side) not in real_positions:
                        log.info("Removing old position: %s %s", db_sym, db_side)
                        pg_delete_position("positions", db_sym, db_side)

            # b) orders
            with pg_conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT symbol, position_side, order_id FROM public.orders")
                rows = cur.fetchall()
                for (db_sym, db_side, db_oid) in rows:
                    if (db_sym, db_side, db_oid) not in real_orders:
                        log.info("Removing old order: %s %s %s", db_sym, db_side, db_oid)
                        pg_delete_order(db_sym, db_side, db_oid)

        except Exception as e:
            log.error("_sync_start: %s", e)

    def _ws_handler(self, msg:Dict[str,Any]):
        pg_raw(msg)
        log.debug("[WS] %s", msg)
        if msg.get("e")=="ORDER_TRADE_UPDATE":
            self._on_order(msg["o"])

    def _on_order(self, o:Dict[str,Any]):
        """
        CANCEL => pg_delete_order
        NEW => pg_upsert_order
        FILLED => remove from orders + update positions + mirror
        """
        sym    = o["s"]
        otype  = o["ot"]
        status = o["X"]
        fill_price= float(o.get("ap",0))
        fill_qty  = float(o.get("l",0))
        reduce_flag= bool(o.get("R",False))
        partial_pnl= float(o.get("rp",0.0))
        side = decode_side(o)  # "LONG"/"SHORT"
        order_id= int(o.get("i",0))

        if status=="CANCELED":
            pg_delete_order(sym, side, order_id)
            p= float(o.get("p",0))
            q= float(o.get("q",0))
            tg_a(f"{pos_color(side)} Trader: {sym} {otype} отменён, qty={q}, price={p}")
            return

        elif status=="NEW":
            # child STOP/TAKE or LIMIT
            from db import pg_upsert_order
            orig_qty= float(o.get("q",0))
            sp= float(o.get("sp",0))
            lp= float(o.get("p",0))

            if otype in CHILD_TYPES:
                main_p= sp if sp>1e-12 else lp
                pg_upsert_order(sym, side, order_id, orig_qty, main_p, "NEW")
                # можно вывести telegram
                kind= "STOP" if "STOP" in otype else "TAKE"
                tg_a(f"{child_color()} Trader: {sym} {side_text(side)} {kind} (NEW), price={main_p}")
            else:
                pg_upsert_order(sym, side, order_id, orig_qty, lp, "NEW")
                tg_a(f"{pos_color(side)} Trader: {sym} {side_text(side)} LIMIT (NEW), price={lp}, qty={orig_qty}")
            return

        elif status=="FILLED":
            if otype in CHILD_TYPES or otype=="LIMIT":
                from db import pg_delete_order
                pg_delete_order(sym, side, order_id)

            if fill_qty<1e-12:
                return

            # STOP => "STOP активирован"
            if otype in CHILD_TYPES:
                s_p= float(o.get("sp",0))
                k= "STOP" if "STOP" in otype else "TAKE"
                tg_a(f"{child_color()} Trader: {sym} {side_text(side)} {k} активирован (stopPrice={s_p}, fill={fill_price})")

            old_amt, old_entry, old_rpnl= pg_get_position("positions", sym, side) or (0.0,0.0,0.0)
            new_rpnl= old_rpnl + partial_pnl

            if reduce_flag:
                # закрытие
                new_amt= old_amt- fill_qty
                ratio= (fill_qty/old_amt)*100 if old_amt>1e-12 else 100
                if ratio>100: ratio=100
                if new_amt<=1e-8:
                    tg_a(f"{pos_color(side)} Trader: {sym} полное закрытие {side_text(side)}, ratio={int(ratio)}%, price={_fmt_float(fill_price)}, PNL={_fmt_float(new_rpnl)}")
                    pg_delete_position("positions", sym, side)
                else:
                    tg_a(f"{pos_color(side)} Trader: {sym} частичное закрытие {side_text(side)}, ratio={int(ratio)}%, old={old_amt}->{new_amt}, price={_fmt_float(fill_price)}, PNL={_fmt_float(new_rpnl)}")
                    pg_upsert_position("positions", sym, side, new_amt, old_entry, new_rpnl, "binance", False)

                if MIRROR_ENABLED:
                    self._mirror_reduce(sym, side, fill_qty, fill_price, partial_pnl)
            else:
                # открытие/увеличение
                new_amt= old_amt+ fill_qty
                if old_amt<1e-12:
                    tg_a(f"{pos_color(side)} Trader: {sym} Открыта позиция {side_text(side)} {reason_text(otype)}, qty={fill_qty}, price={fill_price}")
                else:
                    ratio= (fill_qty/old_amt)*100 if old_amt>1e-12 else 100
                    if ratio>100: ratio=100
                    tg_a(f"{pos_color(side)} Trader: {sym} Увеличение {side_text(side)} ({int(ratio)}%), old={old_amt}->{new_amt}, price={fill_price}")

                pg_upsert_position("positions", sym, side, new_amt, fill_price, new_rpnl, "binance", False)
                if MIRROR_ENABLED:
                    self._mirror_increase(sym, side, fill_qty, fill_price, reason_text(otype))

    def _mirror_reduce(self, sym:str, side:str, fill_qty:float, fill_price:float, partial_pnl:float):
        old_m_amt, old_m_entry, old_m_rpnl= pg_get_position("mirror_positions", sym, side) or (0.0,0.0,0.0)
        dec_qty= fill_qty*MIRROR_COEFFICIENT
        new_m_pnl= old_m_rpnl + partial_pnl*MIRROR_COEFFICIENT
        new_m_amt= old_m_amt- dec_qty

        ratio= (dec_qty/old_m_amt)*100 if old_m_amt>1e-12 else 100
        if ratio>100: ratio=100
        side_binance= "BUY" if side=="SHORT" else "SELL"

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
            tg_m(f"[Mirror] {pos_color(side)} Trader: {sym} полное закрытие {side_text(side)}, ratio={int(ratio)}%, price={fill_price}")
        else:
            pg_upsert_position("mirror_positions", sym, side, new_m_amt, old_m_entry, new_m_pnl, "mirror", False)
            tg_m(f"[Mirror] {pos_color(side)} Trader: {sym} частичное закрытие {side_text(side)}, ratio={int(ratio)}%, price={fill_price}")

    def _mirror_increase(self, sym:str, side:str, fill_qty:float, fill_price:float, rtxt:str):
        old_m_amt, old_m_entry, old_m_rpnl= pg_get_position("mirror_positions", sym, side) or (0.0,0.0,0.0)
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
            tg_m(f"[Mirror] {pos_color(side)} Trader: {sym} Открыта {side_text(side)} {rtxt}, qty={inc_qty}, price={fill_price}")
        else:
            ratio= (inc_qty/old_m_amt)*100 if old_m_amt>1e-12 else 100
            if ratio>100: ratio=100
            tg_m(f"[Mirror] {pos_color(side)} Trader: {sym} Увеличение {side_text(side)}, ratio={int(ratio)}%, old={old_m_amt}->{new_m_amt}, price={fill_price}")

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