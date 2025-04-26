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

# Типы STOP/TAKE
CHILD_TYPES = {
    "STOP","STOP_MARKET","STOP_LOSS","STOP_LOSS_LIMIT",
    "TAKE_PROFIT","TAKE_PROFIT_LIMIT","TAKE_PROFIT_MARKET"
}

def pos_color(side:str)->str:
    """
    Возвращаем 🔴 (SHORT) или 🟢 (LONG).
    """
    return "🟢" if side=="LONG" else "🔴"

def child_color()->str:
    """
    Для STOP/TAKE используем 🔵
    """
    return "🔵"

def side_name(side:str)->str:
    """
    Русские слова «LONG => ЛОНГ», «SHORT => ШОРТ».
    """
    return "ЛОНГ" if side=="LONG" else "ШОРТ"

def reason_text(otype:str)->str:
    mp = {
        "MARKET":"(MARKET)",
        "LIMIT":"(LIMIT)",
        "STOP":"(STOP)",
        "STOP_MARKET":"(STOP MARKET)",
        "TAKE_PROFIT":"(TAKE PROFIT)",
        "TAKE_PROFIT_MARKET":"(TAKE PROFIT MARKET)"
    }
    return mp.get(otype, f"({otype})")

def _fmt_float(x: float, digits:int=4)->str:
    """
    Форматировать число с digits знаками, убирая нули в конце.
    """
    s = f"{x:.{digits}f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s

def decode_side_ws(o:Dict[str,Any])->str:
    """
    Для WS‑событий:
    if R=true and S=BUY => side=SHORT
    else side=LONG...
    """
    reduce_flag = bool(o.get("R",False))
    raw_side    = o["S"]  # "BUY"/"SELL"
    if reduce_flag:
        if raw_side=="BUY":
            return "SHORT"
        else:
            return "LONG"
    else:
        if raw_side=="BUY":
            return "LONG"
        else:
            return "SHORT"

def decode_side_openorders(raw_side:str, reduce_f:bool, closepos:bool)->str:
    """
    Для _sync_start, open_orders:
      if reduceOnly=true or closePosition=true => BUY => SHORT, SELL => LONG
      else BUY=>LONG, SELL=>SHORT
    """
    if reduce_f or closepos:
        if raw_side=="BUY":
            return "SHORT"
        else:
            return "LONG"
    else:
        if raw_side=="BUY":
            return "LONG"
        else:
            return "SHORT"

class AlexBot:
    """
    Бот c разделением:
     - positions = реальные объёмы
     - orders = лимиты/стопы
    При старте (_sync_start):
      1) Берём futures_position_information => positions
      2) Берём futures_get_open_orders => orders
      3) Удаляем лишнее
    При WS:
      - CANCEL => удаляем orders
      - NEW => upsert orders
      - FILLED => удаляем из orders, обновляем positions + mirror
    Сообщения печатаем в «красивом» русском стиле, как просили.
    """

    def __init__(self):
        log.debug("AlexBot.__init__ called")

        self.client_a = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        self.client_b = (
            Client(MIRROR_B_API_KEY, MIRROR_B_API_SECRET)
            if MIRROR_ENABLED else None
        )

        # Храним точность (lot_size, price_size)
        self.lot_size_map = {}
        self.price_size_map = {}
        self._init_symbol_precisions()

        # Запускаем WS
        self.ws = ThreadedWebsocketManager(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET
        )
        self.ws.start()
        self.ws.start_futures_user_socket(callback=self._ws_handler)

        # сбрасываем mirror, pending
        wipe_mirror()
        reset_pending()
        self._sync_start()
        self._hello()

    # ------ точности ------
    def _init_symbol_precisions(self):
        log.debug("_init_symbol_precisions called")
        try:
            info = self.client_a.futures_exchange_info()
            for s in info["symbols"]:
                sym_name= s["symbol"]
                lot_dec, price_dec=4,4
                for f in s["filters"]:
                    if f["filterType"]=="LOT_SIZE":
                        lot_dec= self._step_to_decimals(f["stepSize"])
                    elif f["filterType"]=="PRICE_FILTER":
                        price_dec= self._step_to_decimals(f["tickSize"])
                self.lot_size_map[sym_name]= lot_dec
                self.price_size_map[sym_name]= price_dec
            log.info("_init_symbol_precisions: loaded %d pairs", len(info["symbols"]))
        except Exception as e:
            log.error("_init_symbol_precisions: %s", e)

    @staticmethod
    def _step_to_decimals(step_str:str)->int:
        s= step_str.rstrip('0')
        if '.' not in s:
            return 0
        return len(s.split('.')[1])

    def _fmt_qty(self, symbol:str, qty:float)->str:
        dec= self.lot_size_map.get(symbol,4)
        val= f"{qty:.{dec}f}"
        return val.rstrip('0').rstrip('.') if '.' in val else val

    def _fmt_price(self, symbol:str, price:float)->str:
        dec= self.price_size_map.get(symbol,4)
        val= f"{price:.{dec}f}"
        return val.rstrip('0').rstrip('.') if '.' in val else val
    # -----------------------

    def _hello(self):
        """
        Приветствие
        """
        bal_a= self._usdt(self.client_a)
        msg= f"▶️  Бот запущен.\nОсновной аккаунт: {_fmt_float(bal_a)} USDT"
        if self.client_b and MIRROR_ENABLED:
            bal_b= self._usdt(self.client_b)
            msg += f"\nЗеркальный аккаунт активен: {_fmt_float(bal_b)} USDT"
        log.info(msg)
        tg_m(msg)

    def _usdt(self, cl:Client)->float:
        try:
            bals= cl.futures_account_balance()
            for b in bals:
                if b["asset"]=="USDT":
                    return float(b["balance"])
        except Exception as e:
            log.error("_usdt: %s", e)
        return 0.0

    def _sync_start(self):
        """
        1) позиции => positions
        2) ордера => orders
        3) лишнее удаляем
        """
        log.debug("_sync_start called")
        try:
            # --- позиции ---
            pos_info= self.client_a.futures_position_information()
            real_positions= set()
            for p in pos_info:
                amt= float(p["positionAmt"])
                if abs(amt)<1e-12:
                    continue
                sym= p["symbol"]
                side= "LONG" if amt>0 else "SHORT"
                price= float(p["entryPrice"])
                vol= abs(amt)
                real_positions.add((sym, side))

                # Пример вывода: 🟢 (start) Trader: ENAUSDT Открыта LONG Объём: 1900, Цена: 0.3423
                txt= (f"{pos_color(side)} (start) Trader: {sym} Открыта {side_name(side)} "
                      f"Объём: {self._fmt_qty(sym, vol)}, Цена: {self._fmt_price(sym, price)}")

                # Далее можно поискать SL/TP
                # (но это child-ордер, мы это делаем в разделе open_orders)
                tg_a(txt)

                # upsert
                pg_upsert_position("positions", sym, side, vol, price, 0.0, "binance", False)

            # --- ордера ---
            from db import pg_upsert_order
            all_orders= self.client_a.futures_get_open_orders()
            real_orders= set()

            for od in all_orders:
                if od["status"]!="NEW":
                    continue
                raw_side= od["side"]   # "BUY"/"SELL"
                reduce_f= bool(od.get("reduceOnly",False))
                closepos= (od.get("closePosition","false")=="true")
                side= decode_side_openorders(raw_side, reduce_f, closepos)

                otype= od["type"] # "STOP_MARKET","TAKE_PROFIT"...
                oid  = int(od["orderId"])
                sym  = od["symbol"]

                orig_qty= float(od.get("origQty",0))
                stp     = float(od.get("stopPrice",0))
                lmt     = float(od.get("price",0))

                # Для STOP/TAKE => price=stp
                main_price= stp if (otype in CHILD_TYPES and stp>1e-12) else lmt

                pg_upsert_order(sym, side, oid, orig_qty, main_price, "NEW")
                real_orders.add((sym, side, oid))

                if otype in CHILD_TYPES:
                    # Пример: 🔵 (start) Trader: SKLUSDT ЛОНГ STOP установлен на цену 0.023
                    kind= "STOP" if "STOP" in otype else "TAKE"
                    txt= (f"{child_color()} (start) Trader: {sym} {side_name(side)} "
                          f"{kind} установлен на цену {self._fmt_price(sym, main_price)}")
                else:
                    # LIMIT
                    txt= (f"{pos_color(side)} (start) Trader: {sym} {side_name(side)} "
                          f"LIMIT, Qty: {self._fmt_qty(sym, orig_qty)}, Price: {self._fmt_price(sym, main_price)}")

                tg_a(txt)

            # --- чистим лишнее ---
            from db import pg_delete_order
            # a) positions
            with pg_conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT symbol, position_side FROM public.positions WHERE exchange='binance'")
                rows= cur.fetchall()
                for (db_sym, db_side) in rows:
                    if (db_sym, db_side) not in real_positions:
                        log.info("Removing old pos: %s %s", db_sym, db_side)
                        pg_delete_position("positions", db_sym, db_side)

            # b) orders
            with pg_conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT symbol, position_side, order_id FROM public.orders")
                rows= cur.fetchall()
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
        При событии:
         CANCELED => удаляем из orders, сообщение
         NEW => upsert в orders, сообщение
         FILLED => delete from orders, positions => update, mirror => ...
        """
        sym    = o["s"]
        otype  = o["ot"]
        status = o["X"]
        fill_price= float(o.get("ap",0))
        fill_qty  = float(o.get("l",0))
        reduce_flag= bool(o.get("R",False))
        partial_pnl= float(o.get("rp",0.0))
        order_id= int(o.get("i",0))

        side= decode_side_ws(o)  # "LONG"/"SHORT"

        # CANCEL
        if status=="CANCELED":
            pg_delete_order(sym, side, order_id)
            price= float(o.get("p",0))
            qty  = float(o.get("q",0))
            # Пример: 🔵 Trader: BTCUSDT LIMIT отменён (Был 🔴 SHORT, Объём: 2.0 по цене 16500).
            txt= (f"🔵 Trader: {sym} {otype} отменён. "
                  f"(Был {pos_color(side)} {side_name(side)}, Объём: {self._fmt_qty(sym, qty)} "
                  f"по цене {self._fmt_price(sym, price)})")
            tg_a(txt)
            return

        # NEW
        elif status=="NEW":
            from db import pg_upsert_order
            orig_qty= float(o.get("q",0))
            stp     = float(o.get("sp",0))
            lmt     = float(o.get("p",0))

            if otype in CHILD_TYPES:
                # STOP or TAKE
                price= stp if stp>1e-12 else lmt
                pg_upsert_order(sym, side, order_id, orig_qty, price, "NEW")
                kind= "STOP" if "STOP" in otype else "TAKE"
                # Пример: 🔵 Trader: 1000PEPEUSDT STOP установлен на цену 0.009
                txt= (f"🔵 Trader: {sym} {kind} установлен на цену {self._fmt_price(sym, price)}")
                tg_a(txt)
            else:
                # LIMIT
                pg_upsert_order(sym, side, order_id, orig_qty, lmt, "NEW")
                txt= (f"🔵 Trader: {sym} Новый LIMIT {pos_color(side)} {side_name(side)}. "
                      f"Объём: {self._fmt_qty(sym, orig_qty)} по цене {self._fmt_price(sym, lmt)}.")
                tg_a(txt)
            return

        # FILLED
        elif status=="FILLED":
            # Удаляем из orders, если LIMIT/STOP
            if otype in CHILD_TYPES or otype=="LIMIT":
                pg_delete_order(sym, side, order_id)

            if fill_qty<1e-12:
                return

            # Если STOP => "STOP активирован"
            if otype in CHILD_TYPES:
                stp= float(o.get("sp",0))
                kind= "STOP" if "STOP" in otype else "TAKE"
                # 🔵 Trader: ETHUSDT STOP активирован по цене 1818 (факт. исполнение 1829.34)
                txt= (f"🔵 Trader: {sym} {kind} активирован по цене {self._fmt_price(sym, stp)} "
                      f"(факт. исполнение {self._fmt_price(sym, fill_price)})")
                tg_a(txt)

            # Обновляем positions
            old_amt, old_entry, old_rpnl= pg_get_position("positions", sym, side) or (0.0,0.0,0.0)
            new_rpnl= old_rpnl + partial_pnl

            if reduce_flag:
                new_amt= old_amt- fill_qty
                ratio= 100
                if old_amt>1e-12:
                    ratio= (fill_qty/old_amt)*100
                if ratio>100: ratio=100

                if new_amt<=1e-8:
                    # полное закрытие
                    # 🔴 Trader: ETHUSDT полное закрытие позиции SHORT (100%, 0.764 --> 0) ...
                    txt= (f"{pos_color(side)} Trader: {sym} полное закрытие позиции {side_name(side)} "
                          f"({int(ratio)}%, {_fmt_float(old_amt)} --> 0) по цене {self._fmt_price(sym, fill_price)}, "
                          f"общий PNL: {_fmt_float(new_rpnl)}")
                    tg_a(txt)
                    pg_delete_position("positions", sym, side)
                else:
                    # частичное закрытие
                    # 🔵 Trader: ENAUSDT частичное закрытие позиции LONG (50%, 1900 --> 950) по цене ...
                    txt= (f"{pos_color(side)} Trader: {sym} частичное закрытие позиции {side_name(side)} "
                          f"({int(ratio)}%, {_fmt_float(old_amt)} --> {_fmt_float(new_amt)}) "
                          f"по цене {self._fmt_price(sym, fill_price)}, текущий PNL: {_fmt_float(new_rpnl)}")
                    tg_a(txt)
                    pg_upsert_position("positions", sym, side, new_amt, old_entry, new_rpnl, "binance", False)

                if MIRROR_ENABLED:
                    self._mirror_reduce(sym, side, fill_qty, fill_price, partial_pnl)

            else:
                # открытие/увеличение
                new_amt= old_amt + fill_qty
                ratio= 100
                if old_amt>1e-12:
                    ratio= (fill_qty/old_amt)*100
                    if ratio>100: ratio=100

                if old_amt<1e-12:
                    # Открыта позиция SHORT (MARKET) на 1000 по цене ...
                    txt= (f"{pos_color(side)} Trader: {sym} Открыта позиция {side_name(side)} "
                          f"{reason_text(otype)} на {self._fmt_qty(sym, fill_qty)} "
                          f"по цене {self._fmt_price(sym, fill_price)}")
                else:
                    # Увеличение
                    # 🔴 Trader: 1000PEPEUSDT Увеличение позиции SHORT (100%, 1000 --> 2000) ...
                    txt= (f"{pos_color(side)} Trader: {sym} Увеличение позиции {side_name(side)} "
                          f"({int(ratio)}%, {_fmt_float(old_amt)} --> {_fmt_float(new_amt)}) "
                          f"по цене {self._fmt_price(sym, fill_price)}")
                tg_a(txt)
                pg_upsert_position("positions", sym, side, new_amt, fill_price, new_rpnl, "binance", False)

                if MIRROR_ENABLED:
                    self._mirror_increase(sym, side, fill_qty, fill_price, reason_text(otype))

    def _mirror_reduce(self, sym:str, side:str, fill_qty:float, fill_price:float, partial_pnl:float):
        """
        [Mirror]: 🔴 Trader: ETHUSDT полное закрытие позиции SHORT ...
        """
        old_m_amt, old_m_entry, old_m_rpnl= pg_get_position("mirror_positions", sym, side) or (0.0,0.0,0.0)
        dec_qty= fill_qty * MIRROR_COEFFICIENT
        new_m_pnl= old_m_rpnl + partial_pnl*MIRROR_COEFFICIENT
        new_m_amt= old_m_amt - dec_qty

        ratio= 100
        if old_m_amt>1e-12:
            ratio= (dec_qty/old_m_amt)*100
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
            txt= (f"[Mirror]: {pos_color(side)} Trader: {sym} "
                  f"полное закрытие позиции {side_name(side)} "
                  f"({int(ratio)}%, {_fmt_float(old_m_amt)} --> 0.0) "
                  f"по цене {self._fmt_price(sym, fill_price)}, PNL: {_fmt_float(new_m_pnl)}")
            tg_m(txt)
        else:
            pg_upsert_position("mirror_positions", sym, side, new_m_amt, old_m_entry, new_m_pnl, "mirror", False)
            txt= (f"[Mirror]: {pos_color(side)} Trader: {sym} "
                  f"частичное закрытие позиции {side_name(side)} "
                  f"({int(ratio)}%, {_fmt_float(old_m_amt)} --> {_fmt_float(new_m_amt)}) "
                  f"по цене {self._fmt_price(sym, fill_price)}, PNL: {_fmt_float(new_m_pnl)}")
            tg_m(txt)

    def _mirror_increase(self, sym:str, side:str, fill_qty:float, fill_price:float, rtxt:str):
        """
        [Mirror]: 🔴 Trader: PEPEUSDT Открыта позиция SHORT (MARKET) ...
        """
        old_m_amt, old_m_entry, old_m_rpnl= pg_get_position("mirror_positions", sym, side) or (0.0,0.0,0.0)
        inc_qty= fill_qty*MIRROR_COEFFICIENT
        new_m_amt= old_m_amt+ inc_qty

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
            txt= (f"[Mirror]: {pos_color(side)} Trader: {sym} Открыта позиция {side_name(side)} "
                  f"{rtxt} на {self._fmt_qty(sym, inc_qty)} по цене {self._fmt_price(sym, fill_price)}")
            tg_m(txt)
        else:
            ratio= 100
            if old_m_amt>1e-12:
                ratio= (inc_qty/old_m_amt)*100
            if ratio>100: ratio=100
            txt= (f"[Mirror]: {pos_color(side)} Trader: {sym} Увеличение позиции {side_name(side)} "
                  f"({int(ratio)}%, {old_m_amt}->{new_m_amt}) {rtxt} по цене {self._fmt_price(sym, fill_price)}")
            tg_m(txt)

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