import psycopg2
import json
import logging
from typing import Dict, Any
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

log = logging.getLogger(__name__)

def pg_conn():
    """
    Создаёт новое соединение к PostgreSQL на основе данных из config.py.
    """
    if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
        raise RuntimeError("Не заданы все переменные окружения для PostgreSQL")
    dsn = (
        f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD} sslmode=require"
    )
    return psycopg2.connect(dsn)

def pg_raw(msg: Dict[str, Any]):
    """
    Сохраняем ВСЁ WS‑сообщение в таблицу futures_events.
    """
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.futures_events
                       (exchange, event_type, symbol, raw_data)
                VALUES (%s, %s, %s, %s)
            """, (
                "binance",
                msg.get("e"),                    # event_type
                msg.get("o", {}).get("s"),       # symbol
                json.dumps(msg)                  # raw_data
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
    Флаг pending = True для ещё не исполненных LIMIT‑ордеров.
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
                 updated_at    = now();
            """, (exchange, symbol, side, amt, price, pnl, pending))
    except Exception as e:
        log.error("pg_upsert_position[%s]: %s", table, e)

def wipe_mirror():
    """
    Очищаем таблицу mirror_positions при старте (truncate).
    """
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE public.mirror_positions;")
    except Exception as e:
        log.error("wipe_mirror: %s", e)

def reset_pending():
    """
    Сбрасываем флаг pending в таблице positions для старых записей.
    """
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute("UPDATE public.positions SET pending=false WHERE exchange='binance';")
    except Exception as e:
        log.error("reset_pending: %s", e)
