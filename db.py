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
    Сбрасываем флаг pending в таблице positions (exchange=binance).
    """
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute("UPDATE public.positions SET pending=false WHERE exchange='binance';")
    except Exception as e:
        log.error("reset_pending: %s", e)