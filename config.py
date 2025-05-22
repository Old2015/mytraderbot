import os
from dotenv import load_dotenv

# ------------------------------------------------------------
# Файл конфигурации. Читает переменные окружения и предоставляет
# их в виде переменных модуля, которые затем импортируются
# другими частями программы.
# ------------------------------------------------------------

# Загружаем переменные окружения из файла .env, если он присутствует
load_dotenv()

# ---- Параметры подключения к PostgreSQL ----
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ---- Настройки Telegram-бота ----
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
MIRROR_B_TG_CHAT_ID = os.getenv("MIRROR_B_TG_CHAT_ID")

# ---- Доступ к Binance ----
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

# ---- Параметры "зеркального" аккаунта ----
MIRROR_ENABLED     = os.getenv("MIRROR_ENABLED", "false").lower() in ("1", "true", "yes")
MIRROR_B_API_KEY   = os.getenv("MIRROR_B_API_KEY")
MIRROR_B_API_SECRET= os.getenv("MIRROR_B_API_SECRET")
MIRROR_COEFFICIENT = float(os.getenv("MIRROR_COEFFICIENT", "1.0"))

# ---- Опции ежемесячного отчёта ----
MONTHLY_REPORT_ENABLED = os.getenv("MONTHLY_REPORT_ENABLED", "true").lower() in ("1", "true", "yes")

# ---- Через сколько дней очищать таблицу futures_events ----
FUTURES_EVENTS_RETENTION_DAYS = int(os.getenv("FUTURES_EVENTS_RETENTION_DAYS", "60"))

