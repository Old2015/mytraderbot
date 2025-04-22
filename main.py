#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from alexbot import AlexBot

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%d-%m-%y %H:%M:%S",
    )
    bot = AlexBot()
    bot.run()
