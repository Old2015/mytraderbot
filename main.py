#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
alexbot  |  2025-04-25
──────────────────────────────────────────────────────────────
• Отслеживает фьючерс‑аккаунт Binance (Account A)
• Зеркалирует сделки на Account B (если MIRROR_ENABLED=true)
• Пишет ВСЁ «сырое» WS‑сообщение в futures_events
• Хранит позиции / mirror‑позиции в БД
• При старте:
    – очищает mirror_positions
    – сбрасывает флаг pending в positions
    – показывает открытые позиции + SL/TP
    – показывает все NEW LIMIT‑ордера как pending
• Дальше — обычная реакция на ORDER_TRADE_UPDATE
"""

import logging
from alexbot import AlexBot

def main():
    # Настраиваем логирование «глобально», чтобы все log.info() попадали
    # и в консоль, и в файл traderbot.log
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%d-%m-%y %H:%M:%S",
        handlers=[
            logging.FileHandler("traderbot.log", mode="a", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

    bot = AlexBot()
    bot.run()

if __name__=="__main__":
    main()