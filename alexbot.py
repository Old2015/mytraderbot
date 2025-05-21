import time
import logging
from datetime import datetime, date, timedelta
from typing import Dict, Any

from binance.client import Client
from binance import ThreadedWebsocketManager

from config import (
    BINANCE_API_KEY, BINANCE_API_SECRET,
    MIRROR_ENABLED, MIRROR_B_API_KEY, MIRROR_B_API_SECRET,
    MIRROR_COEFFICIENT,
    MONTHLY_REPORT_ENABLED,
    FUTURES_EVENTS_RETENTION_DAYS,
)
from db import (
    pg_conn, pg_raw,
    pg_upsert_position, pg_delete_position, pg_get_position,
    wipe_mirror, reset_pending,
    pg_upsert_order, pg_delete_order,
    pg_insert_closed_trade, pg_get_closed_trades_for_month,
    pg_purge_old_futures_events,
)
from telegram_bot import tg_a, tg_m
from typing import Optional

log = logging.getLogger(__name__)

CHILD_TYPES = {
    "STOP","STOP_MARKET","STOP_LOSS","STOP_LOSS_LIMIT",
    "TAKE_PROFIT","TAKE_PROFIT_LIMIT","TAKE_PROFIT_MARKET"
}

def pos_color(side: str)->str:
    """🔵 или 🔴, в зависимости от LONG/SHORT."""
    return "🟢" if side=="LONG" else "🔴"

def child_color()->str:
    """Синий кружок для STOP/TAKE."""
    return "🔵"

def side_name(side:str)->str:
    """'ЛОНГ' / 'ШОРТ'."""
    return "ЛОНГ" if side=="LONG" else "ШОРТ"

def reason_text(otype:str)->str:
    """(MARKET), (LIMIT), (STOP), ..."""
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
    """Формат числа с digits знаками, убирая хвосты."""
    s= f"{x:.{digits}f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s

def decode_side_ws(o: Dict[str,Any]) -> str:
    """ORDER_TRADE_UPDATE: if R + S=BUY => SHORT, etc."""
    reduce_flag= bool(o.get("R",False))
    raw_side  = o["S"]  # "BUY"/"SELL"
    if reduce_flag:
        return "SHORT" if raw_side=="BUY" else "LONG"
    else:
        return "LONG" if raw_side=="BUY" else "SHORT"

def decode_side_openorders(raw_side:str, reduce_f:bool, closepos:bool)->str:
    """
    Для _sync_start(open_orders).
    if reduceOnly or closePosition => BUY=>SHORT, SELL=>LONG, иначе BUY=>LONG, SELL=>SHORT.
    """
    if reduce_f or closepos:
        return "SHORT" if raw_side=="BUY" else "LONG"
    else:
        return "LONG" if raw_side=="BUY" else "SHORT"

class AlexBot:
    """
    Бот, где:
     - positions хранит реальные объёмы
     - orders хранит лимит/стоп
    При старте => _sync_start => всё лишнее удаляем
    При WS => NEW/FILLED/CANCELED => обрабатываем.
    """

    def __init__(self):
        log.debug("AlexBot.__init__ called")

        self.mirror_enabled = MIRROR_ENABLED
        if self.mirror_enabled and not (MIRROR_B_API_KEY and MIRROR_B_API_SECRET):
            log.error(
                "MIRROR_ENABLED but MIRROR_B_API_KEY/SECRET not provided; disabling mirror mode"
            )
            self.mirror_enabled = False

        self.client_a = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        self.client_b = (
            Client(MIRROR_B_API_KEY, MIRROR_B_API_SECRET)
            if self.mirror_enabled else None
        )

        self.lot_size_map = {}
        self.price_size_map= {}
        self._init_symbol_precisions()

        # Запуск WebSocket
        self.ws = ThreadedWebsocketManager(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET
        )
        self.ws.start()
        self.ws.start_futures_user_socket(callback=self._ws_handler)

        # Сброс
        wipe_mirror()
        reset_pending()
        self._sync_start()
        self._hello()

        self.last_report_month = None
        self._last_purge_date = None
        # NEW: Выводим "разрешен ли отчёт" + отчёт за прошлый месяц СРАЗУ
        self._monthly_info_at_start()   # <-- вызываем метод

    # ---------- точность ----------
    def _init_symbol_precisions(self):
        log.debug("_init_symbol_precisions called")
        try:
            info= self.client_a.futures_exchange_info()
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
            log.info("_init_symbol_precisions: loaded %d symbols", len(info["symbols"]))
        except Exception as e:
            log.error("_init_symbol_precisions: %s", e)

    @staticmethod
    def _step_to_decimals(step_str:str)->int:
        s= step_str.rstrip('0')
        if '.' not in s:
            return 0
        return len(s.split('.')[1])

    def _fmt_qty(self, sym:str, qty:float)->str:
        dec= self.lot_size_map.get(sym,4)
        val= f"{qty:.{dec}f}"
        return val.rstrip('0').rstrip('.') if '.' in val else val

    def _fmt_price(self, sym:str, price:float)->str:
        dec= self.price_size_map.get(sym,4)
        val= f"{price:.{dec}f}"
        return val.rstrip('0').rstrip('.') if '.' in val else val

    def _hello(self):
        bal_main= self._usdt(self.client_a)
        msg= f"▶️  Бот запущен.\nОсновной аккаунт: {_fmt_float(bal_main)} USDT"
        if self.mirror_enabled:
            bal_m= self._usdt(self.client_b)
            msg += f"\nЗеркальный аккаунт активен: {_fmt_float(bal_m)} USDT"
        log.info(msg)
        tg_m(msg)

    def _usdt(self, cl: Client)->float:
        try:
            bals= cl.futures_account_balance()
            for b in bals:
                if b["asset"]=="USDT":
                    return float(b["balance"])
        except Exception as e:
            log.error("_usdt: %s", e)
        return 0.0

    def _sync_start(self):
        """Сканируем позиции, ордера, удаляем лишнее."""
        log.debug("_sync_start called")
        try:
            # 1) Позиции
            pos_info= self.client_a.futures_position_information()
            real_positions= set()
            for p in pos_info:
                amt= float(p["positionAmt"])
                if abs(amt)<1e-12:
                    continue
                sym= p["symbol"]
                side= "LONG" if amt>0 else "SHORT"
                prc= float(p["entryPrice"])
                vol= abs(amt)
                real_positions.add((sym, side))

                txt= (f"{pos_color(side)} (restart) Trader: {sym} "
                      f"Открыта {side_name(side)}, Объём={self._fmt_qty(sym, vol)}, "
                      f"Цена={self._fmt_price(sym, prc)}")
                tg_m(txt)
                pg_upsert_position("positions", sym, side, vol, prc, 0.0, "binance", False)

            # 2) Ордера
            all_orders= self.client_a.futures_get_open_orders()
            real_orders= set()

            for od in all_orders:
                if od["status"]!="NEW":
                    continue
                raw_side= od["side"]  # "BUY"/"SELL"
                reduce_f= bool(od.get("reduceOnly",False))
                closepos= (od.get("closePosition","false")=="true")
                side= decode_side_openorders(raw_side, reduce_f, closepos)

                otype= od["type"]  # "LIMIT","STOP_MARKET", ...
                oid  = int(od["orderId"])
                sym  = od["symbol"]

                orig_qty= float(od.get("origQty",0))
                stp_price= float(od.get("stopPrice",0))
                limit_price= float(od.get("price",0))

                # Проверка limit-like
                is_limitlike= ("LIMIT" in otype.upper())
                if is_limitlike:
                    # Если limit_price==0 И stp_price==0, пропускаем
                    if limit_price<1e-12 and stp_price<1e-12:
                        log.info("SKIP: limit-like in _sync_start => price=0 sym=%s side=%s qty=%.4f type=%s",
                                 sym, side, orig_qty, otype)
                        continue

                # Определяем главную цену (если это STOP=> stp_price)
                main_price= stp_price if (otype in CHILD_TYPES and stp_price>1e-12) else limit_price

                pg_upsert_order(sym, side, oid, orig_qty, main_price, "NEW")
                real_orders.add((sym, side, oid))

                # Вывод
                if otype in CHILD_TYPES:
                    # STOP/TAKE
                    kind= "STOP" if "STOP" in otype else "TAKE"
                    txt= (f"{child_color()} (restart) Trader: {sym} {side_name(side)} "
                          f"{kind} установлен на цену {self._fmt_price(sym, main_price)}")
                elif is_limitlike:
                    txt= (f"{pos_color(side)} (restart) Trader: {sym} {side_name(side)} LIMIT, "
                          f"Объём: {self._fmt_qty(sym, orig_qty)} по цене {self._fmt_price(sym, main_price)}")
                else:
                    # fallback
                    txt= (f"{pos_color(side)} (restart) Trader: {sym} {side_name(side)} {otype}, "
                          f"qty={orig_qty}, price={main_price}")

                tg_m(txt)

            # 3) Удаляем лишнее
            with pg_conn() as conn, conn.cursor() as cur:
                # positions
                cur.execute("SELECT symbol, position_side FROM public.positions WHERE exchange='binance'")
                rows= cur.fetchall()
                for (db_sym, db_side) in rows:
                    if (db_sym, db_side) not in real_positions:
                        log.info("Removing old pos from DB: %s %s", db_sym, db_side)
                        pg_delete_position("positions", db_sym, db_side)

            with pg_conn() as conn, conn.cursor() as cur:
                # orders
                cur.execute("SELECT symbol, position_side, order_id FROM public.orders")
                rows= cur.fetchall()
                for (db_sym, db_side, db_oid) in rows:
                    if (db_sym, db_side, db_oid) not in real_orders:
                        log.info("Removing old order from DB: %s %s %s", db_sym, db_side, db_oid)
                        pg_delete_order(db_sym, db_side, db_oid)

        except Exception as e:
            log.error("_sync_start: %s", e)


    # NEW: метод, вызываемый при старте для вывода инфы в зеркальный чат
    def _monthly_info_at_start(self):
        """
        1) Пишем: "Отчёт в основную группу каждого 1го числа: [да/нет]" 
        2) Пишем отчёт за ПРЕДЫДУЩИЙ месяц (или "нет данных"), всё в tg_m.
        """
        # 1) Разрешен ли вывод
        if MONTHLY_REPORT_ENABLED:
            line1 = "Отчёт в основную группу первого числа: ВКЛЮЧЕН"
        else:
            line1 = "Отчёт в основную группу первого числа: ОТКЛЮЧЕН"

        # 2) Отчёт за прошлый месяц
        # Определяем предыдущий месяц
        today = date.today()
        if today.month == 1:
            year = today.year - 1
            month = 12
        else:
            year = today.year
            month = today.month - 1

        trades = pg_get_closed_trades_for_month(year, month)
        if not trades:
            # нет данных
            line2 = f"Данных за {month:02d}.{year} нет, отчёт не сформирован."
        else:
            # есть сделки
            lines = []
            lines.append(f"📊 Отчёт за {month:02d}.{year}")
            total=0.0
            for closed_at, symbol, side, volume, pnl in trades:
                dt_str = closed_at.strftime("%d.%m %H:%M")
                lines.append(f"{dt_str} - {symbol} - {side} - Volume: {_fmt_float(volume)} - PNL={_fmt_float(pnl)} usdt")
                total+= float(pnl)
            lines.append(f"Итоговый PNL: {_fmt_float(total)} usdt")
            line2= "\n".join(lines)

        msg = line1 + "\n" + line2
        tg_m(msg)




    def _ws_handler(self, msg:Dict[str,Any]):
        pg_raw(msg)
        log.debug("[WS] %s", msg)
        if msg.get("e")=="ORDER_TRADE_UPDATE":
            self._on_order(msg["o"])

    def _on_order(self, o:Dict[str,Any]):
        sym     = o["s"]
        otype   = o["ot"]   # e.g. "LIMIT","MARKET"
        status  = o["X"]    # "NEW","CANCELED","FILLED"
        fill_price= float(o.get("ap",0))
        fill_qty  = float(o.get("l",0))
        reduce_flag= bool(o.get("R",False))
        partial_pnl= float(o.get("rp",0.0))
        order_id= int(o.get("i",0))

        side= decode_side_ws(o)  # LONG/SHORT

        # Если "NEW", проверим, действительно ли этот ордер есть в openOrders
        if status=="NEW":
            # Это ключевой фикс: чтобы исключить фантом "Новый LIMIT ... price=0"
            # Делаем API-запрос open_orders по symbol
            try:
                open_list= self.client_a.futures_get_open_orders(symbol=sym)
                # Ищем orderId=order_id
                found= any( (int(x["orderId"])==order_id) for x in open_list )
                if not found:
                    # Это фантом
                    log.info("SKIP phantom 'NEW' order => not in openOrders: sym=%s, side=%s, orderId=%d, type=%s", 
                             sym, side, order_id, otype)
                    return
            except Exception as ee:
                log.error("Failed to check openOrders for %s: %s", sym, ee)

        if status=="CANCELED":
            pg_delete_order(sym, side, order_id)
            pr= float(o.get("p",0))
            q= float(o.get("q",0))
            txt= (f"🔵 Trader: {sym} {otype} отменён. "
                  f"(Был {pos_color(side)} {side_name(side)}, Объём: {self._fmt_qty(sym, q)} "
                  f"по цене {self._fmt_price(sym, pr)})")
            tg_a(txt)
            return

        elif status=="NEW":
            # значит это реально существующий (найден в openOrders)
            from db import pg_upsert_order
            orig_qty= float(o.get("q",0))
            stp= float(o.get("sp",0))
            lmt= float(o.get("p",0))

            # is limit-like?
            is_limitlike= ("LIMIT" in otype.upper())
            if is_limitlike:
                # если lmt=0 и stp=0 => skip
                if lmt<1e-12 and stp<1e-12:
                    log.info("SKIP: new limit-like with 0 price => %s side=%s qty=%.4f type=%s", sym, side, orig_qty, otype)
                    return

            if otype in CHILD_TYPES:
                price= stp if stp>1e-12 else lmt
                pg_upsert_order(sym, side, order_id, orig_qty, price, "NEW")
                kind= "STOP" if "STOP" in otype else "TAKE"
                txt= (f"🔵 Trader: {sym} {kind} установлен на цену {self._fmt_price(sym, price)}")
                tg_a(txt)
            else:
                pg_upsert_order(sym, side, order_id, orig_qty, lmt, "NEW")
                txt= (f"🔵 Trader: {sym} Новый LIMIT {pos_color(side)} {side_name(side)}. "
                      f"Объём: {self._fmt_qty(sym, orig_qty)} по цене {self._fmt_price(sym, lmt)}.")
                tg_a(txt)

        elif status=="FILLED":
            # Удаляем из orders, если это limit-like или child
            if (("LIMIT" in otype.upper()) or (otype in CHILD_TYPES)):
                pg_delete_order(sym, side, order_id)

            if fill_qty<1e-12:
                return

            if otype in CHILD_TYPES:
                s_p= float(o.get("sp",0))
                k= "STOP" if "STOP" in otype else "TAKE"
                txt= (f"🔵 Trader: {sym} {k} активирован по цене {self._fmt_price(sym, s_p)} "
                      f"(факт. исполнение {self._fmt_price(sym, fill_price)})")
                tg_a(txt)

            # positions
            old_amt, old_entry, old_rpnl= pg_get_position("positions", sym, side) or (0.0,0.0,0.0)
            new_rpnl= old_rpnl + partial_pnl

            if reduce_flag:
                new_amt= old_amt- fill_qty
                ratio=100
                if old_amt>1e-12:
                    ratio= (fill_qty/old_amt)*100
                if ratio>100: ratio=100

                if new_amt<=1e-8:
                    txt= (f"{pos_color(side)} Trader: {sym} полное закрытие позиции {side_name(side)} "
                          f"({int(ratio)}%, {_fmt_float(old_amt)} --> 0) "
                          f"по цене {self._fmt_price(sym, fill_price)}, общий PNL: {_fmt_float(new_rpnl)}")
                    tg_a(txt)
                    pg_insert_closed_trade(sym, side, old_amt, new_rpnl)
                    pg_delete_position("positions", sym, side)
                else:
                    txt= (f"{pos_color(side)} Trader: {sym} частичное закрытие позиции {side_name(side)} "
                          f"({int(ratio)}%, {_fmt_float(old_amt)} --> {_fmt_float(new_amt)}) "
                          f"по цене {self._fmt_price(sym, fill_price)}, текущий PNL: {_fmt_float(new_rpnl)}")
                    tg_a(txt)
                    pg_upsert_position("positions", sym, side, new_amt, old_entry, new_rpnl, "binance", False)

                if self.mirror_enabled:
                    self._mirror_reduce(sym, side, fill_qty, fill_price, partial_pnl)
            else:
                new_amt= old_amt+ fill_qty
                ratio=100
                if old_amt>1e-12:
                    ratio= (fill_qty/old_amt)*100
                    if ratio>100: ratio=100

                if old_amt<1e-12:
                    txt= (f"{pos_color(side)} Trader: {sym} Открыта позиция {side_name(side)} "
                          f"{reason_text(otype)} на {self._fmt_qty(sym, fill_qty)} "
                          f"по цене {self._fmt_price(sym, fill_price)}")
                else:
                    txt= (f"{pos_color(side)} Trader: {sym} Увеличение позиции {side_name(side)} "
                          f"({int(ratio)}%, {_fmt_float(old_amt)} --> {_fmt_float(new_amt)}) "
                          f"по цене {self._fmt_price(sym, fill_price)}")

                tg_a(txt)
                pg_upsert_position("positions", sym, side, new_amt, fill_price, new_rpnl, "binance", False)

                if self.mirror_enabled:
                    self._mirror_increase(sym, side, fill_qty, fill_price, reason_text(otype))

    def _mirror_reduce(self, sym:str, side:str, fill_qty:float, fill_price:float, partial_pnl:float):
        old_m_amt, old_m_entry, old_m_rpnl= pg_get_position("mirror_positions", sym, side) or (0.0,0.0,0.0)
        dec_qty= fill_qty*MIRROR_COEFFICIENT
        new_m_pnl= old_m_rpnl + partial_pnl*MIRROR_COEFFICIENT
        new_m_amt= old_m_amt- dec_qty

        ratio=100
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
                reduceOnly=True,
            )
        except Exception as e:
            log.error("_mirror_reduce: %s", e)
            return
        if new_m_amt<=1e-8:
            pg_delete_position("mirror_positions", sym, side)
            txt= (f"[Mirror]: {pos_color(side)} Trader: {sym} полное закрытие позиции {side_name(side)} "
                  f"({int(ratio)}%, {_fmt_float(old_m_amt)} --> 0.0) "
                  f"по цене {self._fmt_price(sym, fill_price)}, PNL: {_fmt_float(new_m_pnl)}")
            tg_m(txt)
        else:
            pg_upsert_position("mirror_positions", sym, side, new_m_amt, old_m_entry, new_m_pnl, "mirror", False)
            txt= (f"[Mirror]: {pos_color(side)} Trader: {sym} частичное закрытие позиции {side_name(side)} "
                  f"({int(ratio)}%, {_fmt_float(old_m_amt)} --> {_fmt_float(new_m_amt)}) "
                  f"по цене {self._fmt_price(sym, fill_price)}, PNL: {_fmt_float(new_m_pnl)}")
            tg_m(txt)

    def _mirror_increase(self, sym:str, side:str, fill_qty:float, fill_price:float, rtxt:str):
        old_m_amt, old_m_entry, old_m_rpnl= pg_get_position("mirror_positions", sym, side) or (0.0,0.0,0.0)
        inc_qty= fill_qty*MIRROR_COEFFICIENT
        new_m_amt= old_m_amt+ inc_qty
        side_binance= "BUY" if side=="LONG" else "SELL"

        try:
            self.client_b.futures_create_order(
                symbol=sym,
                side=side_binance,
                type="MARKET",
                quantity=inc_qty,
            )
        except Exception as e:
            log.error("_mirror_increase: %s", e)
            return
        pg_upsert_position("mirror_positions", sym, side, new_m_amt, fill_price, old_m_rpnl, "mirror", False)

        if old_m_amt<1e-12:
            txt= (f"[Mirror]: {pos_color(side)} Trader: {sym} Открыта позиция {side_name(side)} "
                  f"{rtxt} на {self._fmt_qty(sym, inc_qty)} "
                  f"по цене {self._fmt_price(sym, fill_price)}")
            tg_m(txt)
        else:
            ratio=100
            if old_m_amt>1e-12:
                ratio= (inc_qty/old_m_amt)*100
                if ratio>100: ratio=100
            txt= (f"[Mirror]: {pos_color(side)} Trader: {sym} Увеличение позиции {side_name(side)} "
                  f"({int(ratio)}%, {_fmt_float(old_m_amt)} --> {_fmt_float(new_m_amt)}) "
                  f"{rtxt} по цене {self._fmt_price(sym, fill_price)}")
            tg_m(txt)


    def _maybe_monthly_report(self, send_fn=tg_a, prefix: Optional[str] = None):

        if not MONTHLY_REPORT_ENABLED:
            return
        today = datetime.utcnow().date()
        if today.day != 1:
            return
        cur_month = (today.year, today.month)
        if self.last_report_month == cur_month:
            return

        # предыдущий месяц
        if today.month == 1:
            year = today.year - 1
            month = 12
        else:
            year = today.year
            month = today.month - 1

        trades = pg_get_closed_trades_for_month(year, month)
        if not trades:
            self.last_report_month = cur_month
            return


        lines = []
        if prefix:
            lines.append(prefix)
        lines.append(f"📊 Отчёт за {month:02d}.{year}")

        total = 0.0
        for closed_at, symbol, side, volume, pnl in trades:
            dt_str = closed_at.strftime("%d-%m %H:%M")
            lines.append(f"{dt_str} - {symbol} - {_fmt_float(volume)} - {_fmt_float(pnl)}")
            total += float(pnl)
        lines.append(f"Итоговый PNL: {_fmt_float(total)}")

        send_fn("\n".join(lines))

        self.last_report_month = cur_month

    def _maybe_purge_events(self):
        """Purge old futures_events records once per day."""
        today = datetime.utcnow().date()
        if self._last_purge_date == today:
            return
        pg_purge_old_futures_events(FUTURES_EVENTS_RETENTION_DAYS)
        self._last_purge_date = today

    def run(self):
        log.debug("AlexBot.run called")
        try:
            log.info("[Main] bot running ... Ctrl+C to stop")

            # Check monthly report on startup for mirror chat
            self._maybe_monthly_report(send_fn=tg_m, prefix="Вывод в зеркальный чат")
            self._maybe_purge_events()

            while True:
                self._maybe_monthly_report()
                self._maybe_purge_events()
                time.sleep(1)
        except KeyboardInterrupt:
            tg_m("⏹️  Бот остановлен пользователем")
        finally:
            self.ws.stop()
            log.info("[Main] bye.")