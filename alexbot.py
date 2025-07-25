import time
import logging
from datetime import datetime, date, timedelta
import calendar
from typing import Dict, Any, List

# ------------------------------------------------------------
# Основной модуль торгового бота. Здесь реализована логика
# синхронизации позиций, обработка событий от Binance и
# вспомогательные функции.
# ------------------------------------------------------------

from binance.client import Client
from binance import ThreadedWebsocketManager

from config import (
    BINANCE_API_KEY, BINANCE_API_SECRET,
    MIRROR_ENABLED, MIRROR_B_API_KEY, MIRROR_B_API_SECRET,
    MIRROR_COEFFICIENT,
    MONTHLY_REPORT_ENABLED,
    MONTHLY_REPORT_ON_START,
    FUTURES_EVENTS_RETENTION_DAYS,
    REAL_DEPOSIT, FAKE_DEPOSIT, TRADE_FAKE_REPORT,
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

# Эти типы ордеров считаются дочерними (стопы/тейки)

def pos_color(side: str) -> str:
    """Вернуть зелёный или красный кружок в зависимости от LONG/SHORT."""
    return "🟢" if side=="LONG" else "🔴"

def child_color() -> str:
    """Синий кружок для сообщений о стопах/тейках."""
    return "🔵"

def side_name(side: str) -> str:
    """Возвращает строку ``LONG`` или ``SHORT`` в зависимости от стороны."""
    return "LONG" if side == "LONG" else "SHORT"

def reason_text(otype: str) -> str:
    """Return a human friendly name for an order type."""
    mp = {
        "MARKET": "(market order)",
        "LIMIT": "(limit order)",
        "STOP": "(stop order)",
        "STOP_MARKET": "(stop market order)",
        "TAKE_PROFIT": "(take profit order)",
        "TAKE_PROFIT_MARKET": "(take profit market order)",
    }
    return mp.get(otype, f"({otype.lower()} order)")

def _fmt_float(x: float, digits: int = 4) -> str:
    """Форматируем число с плавающей точкой и обрезаем лишние нули."""
    s= f"{x:.{digits}f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s

def _fmt_usdt(x: float, sign: bool = False) -> str:
    """Format number with optional sign and space as thousands separator."""
    fmt = "+,.0f" if sign else ",.0f"
    return format(x, fmt).replace(",", " ")


def _format_monthly_table(
    trades,
    month: int,
    year: int,
    *,
    fake: bool = False,
) -> List[str]:
    """Return formatted monthly report lines."""

    col1_w = 11
    col2_w = 6
    col3_w = 15

    lines: List[str] = []

    month_name = calendar.month_name[month]
    month_abbr = calendar.month_abbr[month]
    days_in_month = calendar.monthrange(year, month)[1]

    lines.append(f"\U0001F4CA Monthly Performance — {month_name} {year}")
    lines.append("")
    lines.append("\u27a1\ufe0f  All trades are listed below in chronological order (earliest \u2192 latest).")
    lines.append(
        f"{'Instrument':<{col1_w}}  {'Side':<{col2_w}}  {'PnL (USDT)':>{col3_w}}"
    )
    lines.append(
        f"{'-'*col1_w}  {'-'*col2_w}  {'-'*col3_w}"
    )

    total_pnl = 0.0
    win_cnt = 0

    for _, symbol, side, reason, volume, pnl, fake_volume, fake_pnl, rr in trades:
        use_pnl = fake_pnl if fake else pnl
        side_word = side.capitalize() if side else ""
        line = (
            f"{symbol:<{col1_w}}  {side_word:<{col2_w}}  "
            f"{_fmt_usdt(use_pnl, sign=True):>{col3_w}}"
        )
        lines.append(line)
        total_pnl += float(use_pnl)
        if use_pnl >= 0:
            win_cnt += 1

    trade_cnt = len(trades)
    loss_cnt = trade_cnt - win_cnt
    win_rate = (win_cnt / trade_cnt) * 100 if trade_cnt else 0

    lines.append("-" * (col1_w + col2_w + col3_w + 4))
    label = f"Total ({trade_cnt} trades)"
    label_w = col1_w + col2_w + 2
    lines.append(f"{label:<{label_w}}  {_fmt_usdt(total_pnl, sign=True):>{col3_w}}")
    lines.append("")
    lines.append(f"Win rate: {win_rate:.0f} %")
    lines.append(f"Winners: {win_cnt} Losers: {loss_cnt}")

    lines.append("")
    lines.append("-" * (col1_w + col2_w + col3_w + 4))

    start_equity = FAKE_DEPOSIT if fake else REAL_DEPOSIT
    end_equity = start_equity + total_pnl
    month_ret = (total_pnl / start_equity) * 100 if start_equity else 0
    apr = month_ret * 12

    sep = "─" * 16  # heavy horizontal line
    indent = "       "  # 7 spaces

    lines.append(sep)
    lines.append("\U0001F4C8 Performance Summary ")
    lines.append(sep)
    lines.append(f"Starting equity (1 {month_name})")
    lines.append(f"{indent}${_fmt_usdt(start_equity)}")
    lines.append("Net P&L for the month ")
    lines.append(
        f"{indent}{('+' if total_pnl>=0 else '-')}${_fmt_usdt(abs(total_pnl))}"
    )
    lines.append(f"Ending equity ({days_in_month} {month_name})")
    lines.append(f"{indent}${_fmt_usdt(end_equity)}")

    lines.append("")
    lines.append(f"Monthly return {month_ret:.2f} %")
    lines.append(f"Annualised return (APR*) {apr:.2f} %")

    lines.append("")
    lines.append("*APR is calculated and is presented")
    lines.append("for illustrative purposes only; future")
    lines.append("returns may differ.")
    lines.append(sep)

    return lines

def decode_side_ws(o: Dict[str,Any]) -> str:
    """Определяем сторону позиции на основе сообщения WS."""
    reduce_flag= bool(o.get("R",False))
    raw_side  = o["S"]  # "BUY"/"SELL"
    if reduce_flag:
        return "SHORT" if raw_side=="BUY" else "LONG"
    else:
        return "LONG" if raw_side=="BUY" else "SHORT"

def decode_side_openorders(raw_side: str, reduce_f: bool, closepos: bool) -> str:
    """Помощник для ``_sync_start`` при разборе открытых ордеров.
    Если выставлен ``reduceOnly`` или ``closePosition`` — направление
    трактуется противоположно (BUY => SHORT)."""
    if reduce_f or closepos:
        return "SHORT" if raw_side=="BUY" else "LONG"
    else:
        return "LONG" if raw_side=="BUY" else "SHORT"

class AlexBot:
    """Торговый бот.
    Хранит текущие объёмы в таблице ``positions`` и лимитные/стоп‑ордера в
    таблице ``orders``. При старте выполняется ``_sync_start``, который очищает
    устаревшие записи, после чего бот обрабатывает события NEW/FILLED/CANCELED
    из WebSocket."""

    def __init__(self):
        log.debug("AlexBot.__init__ called")

        self.use_fake_report = TRADE_FAKE_REPORT
        self.fake_coef = 1.0
        if REAL_DEPOSIT > 0:
            self.fake_coef = FAKE_DEPOSIT / REAL_DEPOSIT

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

        # Словари с точностями для каждого символа
        self.lot_size_map = {}
        self.price_size_map = {}
        # Храним исходные размеры позиций для вычисления процентов
        self.base_sizes = {}
        self.mirror_base_sizes = {}
        # initial position sizes to calculate RR and volume on final close
        self.initial_sizes = {}
        self._init_symbol_precisions()

        # Запуск WebSocket
        self.ws = ThreadedWebsocketManager(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET
        )
        self.ws.start()
        self.ws.start_futures_user_socket(callback=self._ws_handler)

        # Сброс состояния баз в начале работы
        wipe_mirror()
        reset_pending()
        self._sync_start()
        self._hello()

        self.last_report_month = None
        self._last_purge_date = None
        # NEW: immediately show whether the report is enabled and send last month's report
        self._monthly_info_at_start()   # <-- call helper
        if MONTHLY_REPORT_ON_START:
            today = date.today()
            if today.month == 1:
                year = today.year - 1
                month = 12
            else:
                year = today.year
                month = today.month - 1
            month_name = calendar.month_name[month]
            self._send_monthly_summary(
                year,
                month,
                header=f"Monthly report for {month_name} {year}",
                fake=self.use_fake_report,
            )
            self._send_monthly_summary(
                today.year,
                today.month,
                header=f"Monthly report for {calendar.month_name[today.month]} {today.year} (to date)",
                fake=self.use_fake_report,
            )

    # ---------- точность ----------
    def _init_symbol_precisions(self):
        log.debug("_init_symbol_precisions called")
        try:
            # Запрашиваем информацию о бирже, чтобы узнать точности торгов
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
            log.info("_init_symbol_precisions: loaded %d symbols", len(info["symbols"]))
        except Exception as e:
            log.error("_init_symbol_precisions: %s", e)

    @staticmethod
    def _step_to_decimals(step_str:str)->int:
        # Превращаем шаг цены/объёма вида "0.001" в количество знаков после запятой
        s = step_str.rstrip('0')
        if '.' not in s:
            return 0
        return len(s.split('.')[1])

    def _fmt_qty(self, sym:str, qty:float)->str:
        # Форматирование количества с учётом точности символа и добавление названия монеты
        dec = self.lot_size_map.get(sym, 4)
        val = f"{qty:.{dec}f}"
        q = val.rstrip('0').rstrip('.') if '.' in val else val
        coin = sym[:-4] if sym.endswith("USDT") else sym
        return f"{q} {coin}"

    def _display_qty(self, qty: float) -> float:
        """Return quantity scaled for fake report if enabled."""
        return qty * self.fake_coef if self.use_fake_report else qty

    def _fmt_price(self, sym:str, price:float)->str:
        # Форматирование цены с учётом требуемой точности
        dec = self.price_size_map.get(sym, 4)
        val = f"{price:.{dec}f}"
        return val.rstrip('0').rstrip('.') if '.' in val else val

    def _calc_rr(
        self,
        side: str,
        volume: float,
        pnl: float,
        entry_price: float,
        stop_price: float,
        take_price: float,
    ) -> float:
        """Рассчитываем фактическое соотношение риск/прибыль."""
        if (stop_price <= 0.0 and take_price <= 0.0) or stop_price <= 0.0:
            return 1.0 if pnl >= 0 else -1.0

        risk_amount = volume * abs(entry_price - stop_price)
        if risk_amount <= 1e-12:
            return 1.0 if pnl >= 0 else -1.0

        rr = pnl / risk_amount
        return round(rr, 1)

    def _hello(self):
        # Отправляем приветственное сообщение в Telegram
        bal_main = self._usdt(self.client_a)
        msg = f"▶️  Bot started.\nMain account: {_fmt_float(bal_main)} USDT"
        if self.mirror_enabled:
            bal_m = self._usdt(self.client_b)
            msg += f"\nMirror account active: {_fmt_float(bal_m)} USDT"
        log.info(msg)
        tg_m(msg)

    def _usdt(self, cl: Client)->float:
        """Получаем текущий баланс USDT для заданного клиента."""
        try:
            bals = cl.futures_account_balance()
            for b in bals:
                if b["asset"] == "USDT":
                    return float(b["balance"])
        except Exception as e:
            log.error("_usdt: %s", e)
        return 0.0

    def _sync_start(self):
        """Синхронизация состояния при старте бота."""
        log.debug("_sync_start called")
        try:
            # --- 1) Позиции ---
            pos_info= self.client_a.futures_position_information()
            real_positions= set()
            for p in pos_info:
                # Размер открытой позиции
                amt = float(p["positionAmt"])
                if abs(amt)<1e-12:
                    continue
                sym= p["symbol"]
                side= "LONG" if amt>0 else "SHORT"
                prc= float(p["entryPrice"])
                vol= abs(amt)
                real_positions.add((sym, side))
                self.base_sizes[(sym, side)] = vol

                txt = (
                    f"{pos_color(side)} (restart) {sym} "
                    f"{side_name(side)} position opened, Volume={self._fmt_qty(sym, vol)}, "
                    f"Price entry={self._fmt_price(sym, prc)}"
                )
                tg_m(txt)
                pg_upsert_position("positions", sym, side, vol, prc, 0.0, "binance", False)

            # --- 2) Ордера ---
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

                # Output
                if otype in CHILD_TYPES:
                    # STOP/TAKE
                    kind = "STOP" if "STOP" in otype else "TAKE"
                    base_amt = self.base_sizes.get((sym, side)) or 0.0
                    qty_for_calc = orig_qty
                    if closepos and orig_qty < 1e-12:
                        qty_for_calc = base_amt
                    disp_qty = self._display_qty(qty_for_calc)

                    if kind == "TAKE":
                        pct_txt = ""
                        if base_amt > 1e-12 and qty_for_calc > 0:
                            pct = (qty_for_calc / base_amt) * 100
                            pct_txt = f", {pct:.0f}%, Volume {self._fmt_qty(sym, disp_qty)}"
                        elif qty_for_calc > 0:
                            pct_txt = f", Volume {self._fmt_qty(sym, disp_qty)}"
                        txt = (
                            f"{child_color()} (restart) {sym} {side_name(side)} "
                            f"{kind} set at {self._fmt_price(sym, main_price)}{pct_txt}"
                        )
                    else:
                        vol_txt = f", Volume {self._fmt_qty(sym, disp_qty)}" if qty_for_calc > 0 else ""
                        txt = (
                            f"{child_color()} (restart) {sym} {side_name(side)} "
                            f"{kind} set at {self._fmt_price(sym, main_price)}{vol_txt}"
                        )
                elif is_limitlike:
                    txt = (
                        f"{pos_color(side)} (restart) {sym} {side_name(side)} LIMIT, "
                        f"Volume: {self._fmt_qty(sym, orig_qty)} at {self._fmt_price(sym, main_price)}"
                    )
                else:
                    # fallback
                    txt = (
                        f"{pos_color(side)} (restart) {sym} {side_name(side)} {otype}, "
                        f"qty={orig_qty}, price={main_price}"
                    )

                tg_m(txt)

            # --- 3) Удаляем лишнее из БД ---
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


    # NEW: method called on startup to post info to the mirror chat
    def _monthly_info_at_start(self):
        """
        Send a report for the previous month and for the current month
        so far to the mirror chat using real trade data and additionally
        send the same information using fake statistics.
        """

        # Report for the previous month
        # Determine previous month
        today = date.today()
        if today.month == 1:
            year = today.year - 1
            month = 12
        else:
            year = today.year
            month = today.month - 1

        trades = pg_get_closed_trades_for_month(year, month)
        if not trades:
            tg_m(f"No data for {month:02d}.{year}, report not generated.")
        else:
            # real statistics
            lines = []
            lines.append(f"📊 Report for {month:02d}.{year}")
            total_pnl = 0.0
            total_rr = 0.0
            for closed_at, symbol, side, reason, volume, pnl, fake_vol, fake_pnl, rr in trades:
                dt_str = closed_at.strftime("%d.%m %H:%M")
                lines.append(
                    f"{dt_str} - {symbol} - {side} - {reason} - {self._fmt_qty(symbol, volume)} - PnL={_fmt_float(pnl)} usdt - RR={rr:.1f}"
                )
                total_pnl += float(pnl)
                total_rr += float(rr)
            lines.append(f"Total PnL: {_fmt_float(total_pnl)} usdt")
            lines.append(f"Total RR: {total_rr:.1f}")
            tg_m("\n".join(lines))

            # fake statistics
            lines = []
            lines.append(f"\U0001F916 Fake report for {month:02d}.{year}")
            total_pnl = 0.0
            total_rr = 0.0
            for closed_at, symbol, side, reason, volume, pnl, fake_vol, fake_pnl, rr in trades:
                dt_str = closed_at.strftime("%d.%m %H:%M")
                lines.append(
                    f"{dt_str} - {symbol} - {side} - {reason} - {self._fmt_qty(symbol, fake_vol)} - PnL={_fmt_float(fake_pnl)} usdt - RR={rr:.1f}"
                )
                total_pnl += float(fake_pnl)
                total_rr += float(rr)
            lines.append(f"Total PnL: {_fmt_float(total_pnl)} usdt")
            lines.append(f"Total RR: {total_rr:.1f}")
            tg_m("\n".join(lines))

        # --- NEW: report for the current month so far ---
        cur_year = today.year
        cur_month = today.month
        trades_cur = pg_get_closed_trades_for_month(cur_year, cur_month)
        if not trades_cur:
            tg_m(f"No data for {cur_month:02d}.{cur_year} so far.")
        else:
            # real statistics
            lines = []
            lines.append(f"\U0001F4CA Report for {cur_month:02d}.{cur_year} (to date)")
            total_pnl = 0.0
            total_rr = 0.0
            for closed_at, symbol, side, reason, volume, pnl, fake_vol, fake_pnl, rr in trades_cur:
                dt_str = closed_at.strftime("%d.%m %H:%M")
                lines.append(
                    f"{dt_str} - {symbol} - {side} - {reason} - {self._fmt_qty(symbol, volume)} - PnL={_fmt_float(pnl)} usdt - RR={rr:.1f}"
                )
                total_pnl += float(pnl)
                total_rr += float(rr)
            lines.append(f"Total PnL: {_fmt_float(total_pnl)} usdt")
            lines.append(f"Total RR: {total_rr:.1f}")
            tg_m("\n".join(lines))

            # fake statistics for current month
            lines = []
            lines.append(f"\U0001F916 Fake report for {cur_month:02d}.{cur_year} (to date)")
            total_pnl = 0.0
            total_rr = 0.0
            for closed_at, symbol, side, reason, volume, pnl, fake_vol, fake_pnl, rr in trades_cur:
                dt_str = closed_at.strftime("%d.%m %H:%M")
                lines.append(
                    f"{dt_str} - {symbol} - {side} - {reason} - {self._fmt_qty(symbol, fake_vol)} - PnL={_fmt_float(fake_pnl)} usdt - RR={rr:.1f}"
                )
                total_pnl += float(fake_pnl)
                total_rr += float(rr)
            lines.append(f"Total PnL: {_fmt_float(total_pnl)} usdt")
            lines.append(f"Total RR: {total_rr:.1f}")
            tg_m("\n".join(lines))


    def _send_monthly_summary(
        self,
        year: int,
        month: int,
        *,
        header: str,
        send_fn=tg_a,
        fake: bool = False,
    ):
        """Send summary report for the specified month."""
        trades = pg_get_closed_trades_for_month(year, month)
        if not trades:
            send_fn(f"No data for {month:02d}.{year}")
            return

        lines = [header]
        lines.extend(
            _format_monthly_table(trades, month, year, fake=fake)
        )

        send_fn("\n".join(lines))



    def _ws_handler(self, msg:Dict[str,Any]):
        pg_raw(msg)
        log.debug("[WS] %s", msg)
        if msg.get("e")=="ORDER_TRADE_UPDATE":
            self._on_order(msg["o"])

    def _on_order(self, o:Dict[str,Any]):
        """Обработка события ордера из WebSocket."""
        sym     = o["s"]
        otype   = o["ot"]   # e.g. "LIMIT","MARKET"
        status  = o["X"]    # "NEW","CANCELED","FILLED"
        fill_price = float(o.get("ap", 0))  # цена исполнения
        fill_qty = float(o.get("l", 0))     # исполненный объём (часть)
        accum_qty = float(o.get("z", fill_qty))  # суммарно исполненный объём
        reduce_flag = bool(o.get("R", False))
        partial_pnl = float(o.get("rp", 0.0))  # PnL части ордера
        order_id = int(o.get("i", 0))

        # Определяем сторону позиции (LONG/SHORT)
        side = decode_side_ws(o)

        # Если статус NEW, проверим, действительно ли этот ордер есть в openOrders
        if status=="NEW":
            # Это ключевой фикс: чтобы исключить фантом "Новый LIMIT ... price=0"
            # Делаем API-запрос open_orders по symbol
            try:
                open_list = self.client_a.futures_get_open_orders(symbol=sym)
                # Проверяем, присутствует ли orderId в списке открытых ордеров
                found = any(int(x["orderId"]) == order_id for x in open_list)
                if not found:
                    # Это фантом
                    log.info("SKIP phantom 'NEW' order => not in openOrders: sym=%s, side=%s, orderId=%d, type=%s", 
                             sym, side, order_id, otype)
                    return
            except Exception as ee:
                log.error("Failed to check openOrders for %s: %s", sym, ee)

        if status == "CANCELED":
            pg_delete_order(sym, side, order_id)
            pr = float(o.get("p", 0))
            sp = float(o.get("sp", 0))
            q = float(o.get("q", 0))

            if otype in CHILD_TYPES:
                price = sp if sp > 1e-12 else pr
                if "TAKE" in otype:
                    base_amt = (pg_get_position("positions", sym, side) or (0.0,))[0]
                    if base_amt < 1e-12:
                        base_amt = self.base_sizes.get((sym, side)) or 0.0

                    qty_for_calc = q
                    if qty_for_calc < 1e-12 and bool(o.get("cp", False)):
                        qty_for_calc = base_amt

                    pct_txt = ""
                    vol_txt = ""
                    order_word = "take-profit order"
                    if base_amt > 1e-12 and qty_for_calc > 0:
                        pct = (qty_for_calc / base_amt) * 100
                        if pct < 99.99:
                            order_word = "partial take-profit order"
                        pct_txt = f", {pct:.0f}%"
                        vol_txt = f", Volume {self._fmt_qty(sym, self._display_qty(qty_for_calc))}"

                    txt = (
                        f"🔵 {sym} {order_word} canceled. "
                        f"Target was {self._fmt_price(sym, price)}{pct_txt}{vol_txt}."
                    )
                else:
                    txt = (
                        f"🔵 {sym} stop-loss order canceled. "
                        f"Target was {self._fmt_price(sym, price)}."
                    )
            else:
                pct_txt = ""
                vol_txt = ""
                order_word = f"{otype} order"
                if reduce_flag:
                    base_amt = self.base_sizes.get((sym, side)) or (pg_get_position("positions", sym, side) or (0.0,))[0]
                    if base_amt > 1e-12 and q > 0:
                        pct = (q / base_amt) * 100
                        order_word = "take-profit order"
                        if pct < 99.99:
                            order_word = "partial take-profit order"
                        pct_txt = f", {pct:.0f}%"
                        vol_txt = f", Volume {self._fmt_qty(sym, self._display_qty(q))}"
                        txt = (
                            f"🔵 {sym} {order_word} canceled at {self._fmt_price(sym, pr)}{pct_txt}{vol_txt}."
                        )
                    else:
                        disp_q = self._display_qty(q)
                        txt = (
                            f"🔵 {sym} {otype} order canceled. "
                            f"Was {pos_color(side)} {side_name(side)}, volume {self._fmt_qty(sym, disp_q)} "
                            f"at {self._fmt_price(sym, pr)}."
                        )
                else:
                    disp_q = self._display_qty(q)
                    txt = (
                        f"🔵 {sym} {otype} order canceled. "
                        f"Was {pos_color(side)} {side_name(side)}, volume {self._fmt_qty(sym, disp_q)} "
                        f"at {self._fmt_price(sym, pr)}."
                    )
            tg_a(txt)
            return

        elif status == "EXPIRED":
            pg_delete_order(sym, side, order_id)
            pr = float(o.get("p", 0))
            sp = float(o.get("sp", 0))
            q = float(o.get("q", 0))

            if otype in CHILD_TYPES:
                price = sp if sp > 1e-12 else pr
                if "TAKE" in otype:
                    txt = (
                        f"🔵 {sym} take-profit order expired. "
                        f"Target was {self._fmt_price(sym, price)}."
                    )
                else:
                    txt = (
                        f"🔵 {sym} stop-loss order expired. "
                        f"Target was {self._fmt_price(sym, price)}."
                    )
            else:
                disp_q = self._display_qty(q)
                txt = (
                    f"🔵 {sym} {otype} order expired. "
                    f"Was {pos_color(side)} {side_name(side)}, volume {self._fmt_qty(sym, disp_q)} "
                    f"at {self._fmt_price(sym, pr)}."
                )
            tg_a(txt)
            return

        elif status == "NEW":
            # значит это реально существующий (найден в openOrders)
            from db import pg_upsert_order
            orig_qty = float(o.get("q", 0))
            close_pos = bool(o.get("cp", False))
            stp = float(o.get("sp", 0))
            lmt = float(o.get("p", 0))

            # определяем базовый объём позиции
            pos = pg_get_position("positions", sym, side)
            curr_amt = pos[0] if pos else 0.0
            base_amt = curr_amt if curr_amt > 1e-12 else self.base_sizes.get((sym, side)) or 0.0

            if close_pos and orig_qty < 1e-12:
                # для closePosition количество в событии нулевое
                orig_qty = base_amt

            disp_orig_qty = self._display_qty(orig_qty)

            # is limit-like?
            is_limitlike= ("LIMIT" in otype.upper())
            if is_limitlike:
                # если lmt=0 и stp=0 => skip
                if lmt<1e-12 and stp<1e-12:
                    log.info("SKIP: new limit-like with 0 price => %s side=%s qty=%.4f type=%s", sym, side, orig_qty, otype)
                    return

            if otype in CHILD_TYPES:
                price = stp if stp > 1e-12 else lmt
                pg_upsert_order(sym, side, order_id, orig_qty, price, "NEW")
                kind = "STOP" if "STOP" in otype else "TAKE"
                if kind == "TAKE":
                    pct_txt = ""
                    order_word = "take-profit order"
                    if base_amt > 1e-12:
                        pct = (orig_qty / base_amt) * 100
                        if pct < 99.99:
                            order_word = "partial take-profit order"
                        pct_txt = f", {pct:.0f}%, Volume {self._fmt_qty(sym, disp_orig_qty)}"
                    txt = (
                        f"🔵 {sym} {order_word} placed at {self._fmt_price(sym, price)}{pct_txt}."
                    )
                else:
                    txt = (
                        f"🔵 {sym} stop-loss order placed at {self._fmt_price(sym, price)}."
                    )
                tg_a(txt)
            else:
                pg_upsert_order(sym, side, order_id, orig_qty, lmt, "NEW")
                if reduce_flag:
                    base_amt = self.base_sizes.get((sym, side)) or 0.0
                    if base_amt > 1e-12:
                        pct = (orig_qty / base_amt) * 100
                        order_word = "take-profit order"
                        if pct < 99.99:
                            order_word = "partial take-profit order"
                        pct_txt = f", {pct:.0f}%, Volume {self._fmt_qty(sym, disp_orig_qty)}"
                        txt = (
                            f"🔵 {sym} {order_word} placed at {self._fmt_price(sym, lmt)}{pct_txt}."
                        )
                        tg_a(txt)
                        return

                pct_txt = ""
                if reduce_flag:
                    base_amt = self.base_sizes.get((sym, side)) or 0.0
                    if base_amt > 1e-12:
                        pct = (orig_qty / base_amt) * 100
                        pct_txt = f" ({pct:.0f}%)"
                action = "close" if reduce_flag else ""

                side_txt = f"{side_name(side)}{pos_color(side)}"
                order_kind = "closing " if reduce_flag else ""
                txt = (
                    f"🔵 {sym} {side_txt} new {order_kind}limit order: "
                    f"volume {self._fmt_qty(sym, disp_orig_qty)}{pct_txt} at {self._fmt_price(sym, lmt)}."
                )
                tg_a(txt)

        elif status in ("FILLED", "PARTIALLY_FILLED"):
            # Удаляем из orders, если это limit-like или child
            if (("LIMIT" in otype.upper()) or (otype in CHILD_TYPES)):
                pg_delete_order(sym, side, order_id)

            if fill_qty<1e-12:
                return

            if otype in CHILD_TYPES:
                s_p = float(o.get("sp", 0))
                k = "STOP" if "STOP" in otype else "TAKE"
                if k == "TAKE":
                    pos = pg_get_position("positions", sym, side)
                    base_amt = pos[0] if pos else fill_qty
                    pct = 0.0
                    if base_amt > 1e-12:
                        pct = (fill_qty / base_amt) * 100
                    order_word = "take profit order"
                    if pct < 99.99:
                        order_word = "partial take profit order"
                    txt = (
                        f"{pos_color(side)} {sym} {side_name(side)} {order_word} triggered at {self._fmt_price(sym, s_p)}"
                    )
                else:
                    txt = (
                        f"{pos_color(side)} {sym} stop order triggered at {self._fmt_price(sym, s_p)}"
                    )
                tg_a(txt)

            # positions
            old_amt, old_entry, old_rpnl= pg_get_position("positions", sym, side) or (0.0,0.0,0.0)
            new_rpnl= old_rpnl + partial_pnl
            base_amt = self.base_sizes.get((sym, side), old_amt if old_amt>1e-12 else fill_qty)

            if reduce_flag:
                new_amt = old_amt - fill_qty
                ratio = 100
                if old_amt > 1e-12:
                    ratio = (fill_qty / old_amt) * 100
                if ratio > 100:
                    ratio = 100

                # Определяем причину закрытия (тейк/стоп/маркет)
                reason = "market"
                stop_p = 0.0
                take_p = 0.0
                if otype in CHILD_TYPES:
                    sp_val = float(o.get("sp", 0))
                    if "STOP" in otype:
                        stop_p = sp_val
                        reason = "stop"
                    else:
                        take_p = sp_val
                        reason = "take"

                if new_amt <= 1e-8:

                    orig_amt = self.initial_sizes.get((sym, side), old_amt)
                    rr_val = self._calc_rr(side, orig_amt, new_rpnl, old_entry, stop_p, take_p)
                    display_vol = orig_amt * self.fake_coef if self.use_fake_report else orig_amt
                    display_pnl = new_rpnl * self.fake_coef if self.use_fake_report else new_rpnl
                    reason_word = "stop order" if reason == "stop" else ("take profit order" if reason == "take" else "market")
                    txt = (
                        f"{pos_color(side)} {sym} {side_name(side)} position closed 100% by {reason_word} "
                        f"at {self._fmt_price(sym, fill_price)}, Volume: {self._fmt_qty(sym, display_vol)}, "
                        f"PnL: {_fmt_float(display_pnl)} usdt"
                    )
                    tg_a(txt)

                    pg_insert_closed_trade(
                        sym,
                        side,
                        orig_amt,
                        new_rpnl,
                        fake_volume=orig_amt * self.fake_coef,
                        fake_pnl=new_rpnl * self.fake_coef,
                        entry_price=old_entry,
                        exit_price=fill_price,
                        stop_price=stop_p,
                        take_price=take_p,
                        reason=reason,
                        rr=rr_val,
                    )
                    pg_delete_position("positions", sym, side)
                    self.base_sizes.pop((sym, side), None)
                    self.initial_sizes.pop((sym, side), None)
                else:
                    new_pct = 0
                    if base_amt > 1e-12:
                        new_pct = (new_amt / base_amt) * 100
                    closed_pct = 0
                    if base_amt > 1e-12:
                        closed_pct = (fill_qty / base_amt) * 100
                    display_pnl = new_rpnl * self.fake_coef if self.use_fake_report else new_rpnl
                    disp_closed = self._display_qty(fill_qty)
                    disp_left = self._display_qty(new_amt)
                    txt = (
                        f"{pos_color(side)} {sym} {side_name(side)} position decreased "
                        f"-{self._fmt_qty(sym, disp_closed)} (-{int(closed_pct)}%) -> "
                        f"{self._fmt_qty(sym, disp_left)} "
                        f"at {self._fmt_price(sym, fill_price)}, "
                        f"current PnL: {_fmt_float(display_pnl)}"
                    )
                    tg_a(txt)
                    pg_upsert_position("positions", sym, side, new_amt, old_entry, new_rpnl, "binance", False)
                    self.base_sizes[(sym, side)] = new_amt

                if self.mirror_enabled:
                    tg_m(f"[Main] {txt}")
                    self._mirror_reduce(sym, side, fill_qty, fill_price, partial_pnl, reason)

                # warn about outdated protective orders
                self._warn_protective_orders(sym, side, old_amt, new_amt)
            else:
                if old_amt < 1e-12:
                    qty = accum_qty if status == "FILLED" else fill_qty
                    new_amt = qty
                    mirror_amt = qty
                    self.base_sizes[(sym, side)] = new_amt
                    self.initial_sizes[(sym, side)] = new_amt
                    display_vol = self._display_qty(new_amt)

                    txt = (
                        f"{pos_color(side)} {sym} {side_name(side)} position opened "
                        f"{reason_text(otype)} {self._fmt_qty(sym, display_vol)} "
                        f"at {self._fmt_price(sym, fill_price)}"
                    )
                else:
                    new_amt = old_amt + fill_qty
                    mirror_amt = fill_qty
                    disp_add = self._display_qty(fill_qty)
                    disp_new = self._display_qty(new_amt)
                    txt = (
                        f"{pos_color(side)} {sym} {side_name(side)} position increased "
                        f"+{self._fmt_qty(sym, disp_add)} -> "
                        f"{self._fmt_qty(sym, disp_new)} "
                        f"at {self._fmt_price(sym, fill_price)}"
                    )
                    self.base_sizes[(sym, side)] = new_amt
                    self.initial_sizes[(sym, side)] = self.initial_sizes.get((sym, side), old_amt) + fill_qty

                # calculate new average entry price when position size increases
                if new_amt > 1e-12 and old_amt > 1e-12:
                    avg_price = (old_entry * old_amt + fill_price * fill_qty) / new_amt
                else:
                    avg_price = fill_price

                tg_a(txt)
                pg_upsert_position("positions", sym, side, new_amt, avg_price, new_rpnl, "binance", False)

                if self.mirror_enabled:
                    tg_m(f"[Main] {txt}")
                    self._mirror_increase(sym, side, mirror_amt, fill_price, reason_text(otype))

                # warn about outdated protective orders
                self._warn_protective_orders(sym, side, old_amt, new_amt)

    def _mirror_reduce(self, sym: str, side: str, fill_qty: float, fill_price: float, partial_pnl: float, reason: str):
        old_m_amt, old_m_entry, old_m_rpnl = (
            pg_get_position("mirror_positions", sym, side) or (0.0, 0.0, 0.0)
        )
        dec_qty = fill_qty * MIRROR_COEFFICIENT
        new_m_pnl = old_m_rpnl + partial_pnl * MIRROR_COEFFICIENT

        # if database doesn't contain position amount (e.g. after restart),
        # fall back to stored base size so that notifications show correct volume
        if old_m_amt <= 1e-12:
            old_m_amt = self.mirror_base_sizes.get((sym, side), dec_qty)

        new_m_amt = old_m_amt - dec_qty
        base_m_amt = self.mirror_base_sizes.get(
            (sym, side), old_m_amt if old_m_amt > 1e-12 else dec_qty
        )

        ratio = 100
        if old_m_amt > 1e-12:
            ratio = (dec_qty / old_m_amt) * 100
        if ratio > 100:
            ratio = 100
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
            tg_m(f"[Mirror]: failed to close position {sym} {side_name(side)}: {e}")
            return
        if new_m_amt<=1e-8:
            pg_delete_position("mirror_positions", sym, side)
            self.mirror_base_sizes.pop((sym, side), None)
            reason_word = "stop order" if reason == "stop" else ("take profit order" if reason == "take" else "market")
            txt = (
                f"[Mirror]: {pos_color(side)} {sym} {side_name(side)} position closed 100% by {reason_word} "
                f"({int(ratio)}%, {_fmt_float(old_m_amt)} -> 0.0, position 0%) "
                f"at {self._fmt_price(sym, fill_price)}, PnL: {_fmt_float(new_m_pnl)}"
            )
            tg_m(txt)
        else:
            pg_upsert_position("mirror_positions", sym, side, new_m_amt, old_m_entry, new_m_pnl, "mirror", False)
            txt = (
                f"[Mirror]: {pos_color(side)} {sym} {side_name(side)} position decreased "
                f"-{_fmt_float(dec_qty)} (-{int(ratio)}%) -> {_fmt_float(new_m_amt)} "
                f"at {self._fmt_price(sym, fill_price)}, PnL: {_fmt_float(new_m_pnl)}"
            )
            tg_m(txt)
            self.mirror_base_sizes[(sym, side)] = new_m_amt

    def _mirror_increase(self, sym:str, side:str, fill_qty:float, fill_price:float, rtxt:str):
        old_m_amt, old_m_entry, old_m_rpnl= pg_get_position("mirror_positions", sym, side) or (0.0,0.0,0.0)
        inc_qty= fill_qty*MIRROR_COEFFICIENT
        new_m_amt= old_m_amt+ inc_qty
        base_m_amt = self.mirror_base_sizes.get((sym, side), new_m_amt if old_m_amt<1e-12 else old_m_amt)
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
            tg_m(f"[Mirror]: failed to open position {sym} {side_name(side)}: {e}")
            return
        # calculate new average entry price for the mirror account
        if new_m_amt > 1e-12 and old_m_amt > 1e-12:
            m_avg_price = (old_m_entry * old_m_amt + fill_price * inc_qty) / new_m_amt
        else:
            m_avg_price = fill_price

        pg_upsert_position("mirror_positions", sym, side, new_m_amt, m_avg_price, old_m_rpnl, "mirror", False)

        if old_m_amt < 1e-12:
            self.mirror_base_sizes[(sym, side)] = new_m_amt
            txt = (
                f"[Mirror]: {pos_color(side)} {sym} {side_name(side)} position opened "
                f"{rtxt} for {self._fmt_qty(sym, inc_qty)} (100%) "
                f"at {self._fmt_price(sym, fill_price)}"
            )
            tg_m(txt)
        else:
            add_pct = 0
            if base_m_amt > 1e-12:
                add_pct = (inc_qty / base_m_amt) * 100
            txt = (
                f"[Mirror]: {pos_color(side)} {sym} {side_name(side)} position increased "
                f"+{_fmt_float(inc_qty)} ({int(add_pct)}%) -> {_fmt_float(new_m_amt)} "
                f"{rtxt} at {self._fmt_price(sym, fill_price)}"
            )
            tg_m(txt)
            self.mirror_base_sizes[(sym, side)] = new_m_amt


    def _warn_protective_orders(self, symbol: str, side: str, old_amt: float, new_amt: float) -> None:
        """Detect mismatched stop/take orders after position size change."""
        if abs(new_amt - old_amt) <= 1e-8:
            return
        try:
            orders = self.client_a.futures_get_open_orders(symbol=symbol)
        except Exception as e:
            log.error("_warn_protective_orders: %s", e)
            return

        for od in orders:
            if od.get("status") != "NEW":
                continue
            otype = od.get("type", "")
            if otype not in CHILD_TYPES:
                continue
            raw_side = od.get("side", "")
            reduce_f = bool(od.get("reduceOnly", False))
            close_pos = (od.get("closePosition", "false") == "true")
            order_side = decode_side_openorders(raw_side, reduce_f, close_pos)
            if order_side != side:
                continue
            qty = float(od.get("origQty", 0))
            if abs(qty - new_amt) <= 1e-8:
                continue
            kind = "take-profit" if "TAKE_PROFIT" in otype else "stop-loss"
            msg = (
                f"⚠️ {symbol} {side_name(side)}. Position size changed "
                f"from {self._fmt_qty(symbol, self._display_qty(old_amt))} to "
                f"{self._fmt_qty(symbol, self._display_qty(new_amt))}.\n"
                f"Existing {kind} (order {od.get('orderId')}) volume "
                f"{self._fmt_qty(symbol, self._display_qty(qty))}.\n"
                "Please update SL/TP manually."
            )
            tg_m(msg)


    def _maybe_monthly_report(self, send_fn=tg_a, prefix: Optional[str] = None, *, detailed: bool = False, fake: bool = False):
        """Отправка ежемесячного отчёта, если пришёл первый день месяца."""
        if not MONTHLY_REPORT_ENABLED:
            return
        today = datetime.utcnow().date()
        if today.day != 1:
            return
        cur_month = (today.year, today.month)
        if self.last_report_month == cur_month:
            return

        # previous month
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

        # detailed mode retained for mirror chat
        if detailed:
            lines.append(f"📊 Report for {month:02d}.{year}")
            total_pnl = 0.0
            total_rr = 0.0
            for closed_at, symbol, side, reason, volume, pnl, fake_volume, fake_pnl, rr in trades:
                dt_str = closed_at.strftime("%d-%m %H:%M")
                use_pnl = fake_pnl if fake else pnl
                use_vol = fake_volume if fake else volume
                lines.append(
                    f"{dt_str} - {symbol} - {side} - {reason} - {self._fmt_qty(symbol, use_vol)} - PnL={_fmt_float(use_pnl)} usdt - RR={rr:.1f}"
                )
                total_pnl += float(use_pnl)
                total_rr += float(rr)
            lines.append(f"Total PnL: {_fmt_float(total_pnl)} usdt")
            lines.append(f"Total RR: {total_rr:.1f}")
            send_fn("\n".join(lines))
            self.last_report_month = cur_month
            return

        # default output for main channel
        lines.extend(
            _format_monthly_table(trades, month, year, fake=fake)
        )

        send_fn("\n".join(lines))

        self.last_report_month = cur_month

    def _maybe_purge_events(self):
        """Удаляем старые записи из таблицы событий раз в сутки."""
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
            self._maybe_monthly_report(send_fn=tg_m, prefix="Mirror chat output", detailed=True, fake=False)
            self._maybe_purge_events()

            # Основной цикл бота
            while True:
                self._maybe_monthly_report(fake=self.use_fake_report)
                self._maybe_purge_events()
                time.sleep(1)
        except KeyboardInterrupt:
            tg_m("⏹️  Bot stopped by user")
        finally:
            self.ws.stop()
            log.info("[Main] bye.")