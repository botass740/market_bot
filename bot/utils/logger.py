# bot/utils/logger.py

from __future__ import annotations

import logging


def setup_logger(level: int = logging.INFO) -> None:
    """
    Настройка логгера для проекта.
    Без наворотов: консольный вывод, единый формат.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )