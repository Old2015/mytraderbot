import psycopg2
import json
import logging
from typing import Dict, Any, Optional, Tuple
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

log = logging.getLogger(__name__)

def pg_conn():
    if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
        raise RuntimeError("Postgres env-vars incomplete")
    dsn = (
        f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD} sslmode=require"
    )
    return psycopg2.connect(dsn)

def pg_upsert_order(symbol: str,
                    side: str,
                    order_id: int,
                    qty: float,
                    price: float,
                    status: str = "NEW"):
    """
    Добавляем/обновляем запись в таблицу orders по ключу (symbol, side, order_id).
    """
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute("""
              INSERT INTO public.orders (symbol, position_side, order_id,
                                         qty, price, status)
              VALUES (%s, %s, %s, %s, %s, %s)
              ON CONFLICT (symbol, position_side, order_id)
              DO UPDATE SET
                qty     = EXCLUDED.qty,
                price   = EXCLUDED.price,
                status  = EXCLUDED.status,
                updated_at = now();
            """, (symbol, side, order_id, qty, price, status))
    except Exception as e:
        log.error("pg_upsert_order: %s", e)

def pg_delete_order(symbol: str, side: str, order_id: int):
    """
    Удаляем конкретный лимит-ордер.
    """
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                DELETE FROM public.orders
                 WHERE symbol=%s
                   AND position_side=%s
                   AND order_id=%s
            """, (symbol, side, order_id))
    except Exception as e:
        log.error("pg_delete_order: %s", e)
        
def pg_raw(msg: Dict[str, Any]):
    """
    Сохраняем ВСЁ WS‑сообщение в futures_events.
    """
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.futures_events
                       (exchange, event_type, symbol, raw_data)
                VALUES (%s, %s, %s, %s)
            """, (
                "binance",
                msg.get("e"),
                msg.get("o", {}).get("s"),
                json.dumps(msg)
            ))
    except Exception as e:
        log.error("pg_raw: %s", e)

def pg_upsert_position(
    table: str,
    symbol: str,
    side: str,
    amt: float,
    price: float,
    pnl: float = 0.0,
    exchange: str = "binance",
    pending: bool = False
):
    """
    UPSERT в таблицы positions / mirror_positions по ключу (symbol, position_side).
    """
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(f"""
              INSERT INTO public.{table}
                     (exchange, symbol, position_side,
                      position_amt, entry_price, realized_pnl, pending)
              VALUES (%s, %s, %s, %s, %s, %s, %s)
              ON CONFLICT (symbol, position_side)
              DO UPDATE SET
                 exchange      = EXCLUDED.exchange,
                 position_amt  = EXCLUDED.position_amt,
                 entry_price   = EXCLUDED.entry_price,
                 realized_pnl  = EXCLUDED.realized_pnl,
                 pending       = EXCLUDED.pending,
                 updated_at    = now()
            """, (exchange, symbol, side, amt, price, pnl, pending))
    except Exception as e:
        log.error("pg_upsert_position[%s]: %s", table, e)

def pg_delete_position(table: str, symbol: str, side: str):
    """
    Удалить строку из таблицы (positions или mirror_positions).
    """
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM public.{table} WHERE symbol=%s AND position_side=%s",
                (symbol, side)
            )
    except Exception as e:
        log.error("pg_delete_position[%s]: %s", table, e)

def pg_get_position(table: str, symbol: str, side: str) -> Optional[Tuple[float, float, float]]:
    """
    Вернуть (amt, entry_price, realized_pnl) или None, если записи нет.
    """
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(f"""
                SELECT position_amt, entry_price, realized_pnl
                  FROM public.{table}
                 WHERE symbol=%s AND position_side=%s
            """, (symbol, side))
            row = cur.fetchone()
            if row:
                return (float(row[0] or 0), float(row[1] or 0), float(row[2] or 0))
    except Exception as e:
        log.error("pg_get_position[%s]: %s", table, e)
    return None

def wipe_mirror():
    """
    Очищаем таблицу mirror_positions.
    """
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE public.mirror_positions;")
    except Exception as e:
        log.error("wipe_mirror: %s", e)

def reset_pending():
    """
    Сбрасываем флаг pending в таблице positions (exchange='binance').
    """
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute("UPDATE public.positions SET pending=false WHERE exchange='binance';")
    except Exception as e:
        log.error("reset_pending: %s", e)

def pg_insert_closed_trade(
    symbol: str,
    side: str,
    volume: float,
    pnl: float,
    *,
    created_at=None,
    closed_at=None,
    entry_price: float = 0.0,
    exit_price: float = 0.0,
    stop_price: float = 0.0,
    take_price: float = 0.0,
    reason: str = "market",
    rr: float = 0.0,
):
    """Записываем информацию о закрытой сделке."""
    from datetime import datetime

    if closed_at is None:
        closed_at = datetime.utcnow()
    if created_at is None:
        created_at = closed_at

    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.closed_trades
                       (symbol, position_side, volume, pnl,
                        closed_at, created_at,
                        entry_price, exit_price,
                        stop_price, take_price,
                        reason, rr)
                VALUES (%s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s)
                """,
                (
                    symbol,
                    side,
                    volume,
                    pnl,
                    closed_at,
                    created_at,
                    entry_price,
                    exit_price,
                    stop_price,
                    take_price,
                    reason,
                    rr,
                ),
            )
    except Exception as e:
        log.error("pg_insert_closed_trade: %s", e)

def pg_get_closed_trades_for_month(year: int, month: int):
    """Возвращает список закрытых сделок за указанный месяц."""
    from datetime import datetime
    if month == 12:
        start = datetime(year, month, 1)
        end = datetime(year + 1, 1, 1)
    else:
        start = datetime(year, month, 1)
        end = datetime(year, month + 1, 1)
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT closed_at,
                       symbol,
                       position_side,
                       reason,
                       volume,
                       pnl,
                       rr
                  FROM public.closed_trades
                 WHERE closed_at >= %s AND closed_at < %s
                 ORDER BY closed_at
                """,
                (start, end),
            )
            return cur.fetchall()
    except Exception as e:
        log.error("pg_get_closed_trades_for_month: %s", e)
        return []


def pg_purge_old_futures_events(days: int):
    """Delete futures_events entries older than the specified number of days."""
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM public.futures_events
                      WHERE created_at < now() - (%s || ' days')::interval
                """,
                (days,),
            )
    except Exception as e:
        log.error("pg_purge_old_futures_events: %s", e)
