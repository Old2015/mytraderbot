import psycopg2
import json
import logging
from typing import Dict, Any
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
    # NEW: логируем вызов
    log.debug("pg_raw called with msg=%s", msg)
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            query = """
                INSERT INTO public.futures_events
                       (exchange, event_type, symbol, raw_data)
                VALUES (%s, %s, %s, %s)
            """
            log.debug("Executing SQL: %s", query)
            cur.execute(
                query,
                (
                    "binance",
                    msg.get("e"),
                    msg.get("o", {}).get("s"),
                    json.dumps(msg)
                )
            )
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
    # NEW: логируем вызов
    log.debug("pg_upsert_position(%s, %s, %s, amt=%.6f, price=%.6f, pnl=%.6f, exchange=%s, pending=%s)",
              table, symbol, side, amt, price, pnl, exchange, pending)
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            query = f"""
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
            """
            log.debug("Executing SQL: %s", query)
            cur.execute(query, (exchange, symbol, side, amt, price, pnl, pending))
    except Exception as e:
        log.error("pg_upsert_position[%s]: %s", table, e)

def wipe_mirror():
    """
    Очищаем таблицу mirror_positions.
    """
    # NEW: логируем вызов
    log.debug("wipe_mirror called")
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            q = "TRUNCATE public.mirror_positions;"
            log.debug("Executing SQL: %s", q)
            cur.execute(q)
    except Exception as e:
        log.error("wipe_mirror: %s", e)

def reset_pending():
    """
    Сбрасываем флаг pending в таблице positions (exchange=binance).
    """
    # NEW: логируем вызов
    log.debug("reset_pending called")
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            q = "UPDATE public.positions SET pending=false WHERE exchange='binance';"
            log.debug("Executing SQL: %s", q)
            cur.execute(q)
    except Exception as e:
        log.error("reset_pending: %s", e)