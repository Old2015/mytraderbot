#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from alexbot import AlexBot

def main():
    # NEW: Настройка "глобального" логгирования на DEBUG,
    #      пишем и в файл traderbot.log, и в консоль
    logging.basicConfig(
        level=logging.DEBUG,  # CHANGED: DEBUG вместо INFO
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%d-%m-%y %H:%M:%S",
        handlers=[
            logging.FileHandler("traderbot.log", mode="a", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

    # NEW: Включаем DEBUG-логгирование для python-binance (очень подробные логи)
    logging.getLogger("binance").setLevel(logging.DEBUG)

    # OPTIONAL: Включить логи запросов HTTP (urllib3). Может быть очень шумно.
    # import urllib3
    # logging.getLogger("urllib3").setLevel(logging.DEBUG)

    bot = AlexBot()
    bot.run()

if __name__=="__main__":
    main()