#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from logging.handlers import RotatingFileHandler
from alexbot import AlexBot

# ------------------------------------------------------------
# Точка входа в приложение. Настраивает логирование и запускает
# основной торговый бот.
# ------------------------------------------------------------

def main():
    # Создаём «корневой» логгер
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Общий уровень DEBUG

    # 1) Пишем всё в файл (до 10 МБ) с ротацией
    fh = RotatingFileHandler(
        "traderbot.log",
        mode="a",
        maxBytes=10_000_000,  # 10 MB
        backupCount=3,
        encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)  # В файл уходит всё (DEBUG+)
    fh.setFormatter(logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%d-%m-%y %H:%M:%S"
    ))
    logger.addHandler(fh)

    # 2) Пишем в консоль только INFO и выше
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%d-%m-%y %H:%M:%S"
    ))
    logger.addHandler(ch)

    # Если логи от python-binance слишком подробные — можно их приглушить:
    # logging.getLogger("binance").setLevel(logging.INFO)

    bot = AlexBot()
    bot.run()

if __name__ == "__main__":
    # Запуск при выполнении файла как скрипта
    main()