import logging
import requests
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, MIRROR_B_TG_CHAT_ID

# ------------------------------------------------------------
# Минимальный клиент Telegram. Используется для отправки сообщений
# в основной и зеркальный чаты.
# ------------------------------------------------------------

log = logging.getLogger(__name__)

def tg_send(chat_id: str, text: str):
    """Отправить текстовое сообщение в Telegram."""
    if not (TELEGRAM_BOT_TOKEN and chat_id):
        return
    try:
        # Выполняем POST-запрос к API Telegram
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10
        )
        if not response.ok:
            # При неуспешном ответе логируем ошибку
            log.error(
                "tg_send failed: status=%s text=%s",
                response.status_code,
                response.text,
            )
    except Exception as e:
        log.error("tg_send: %s", e)

def tg_a(txt: str):
    """Отправить сообщение в основной чат и записать его в лог."""
    log.info(f"[tg_a] {txt}")
    tg_send(TELEGRAM_CHAT_ID, txt)

def tg_m(txt: str):
    """Отправить сообщение в зеркальный чат и записать его в лог."""
    log.info(f"[tg_m] {txt}")
    tg_send(MIRROR_B_TG_CHAT_ID, txt)