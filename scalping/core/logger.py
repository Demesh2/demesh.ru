"""
Настройка логгера с rotating file handler.
"""

import logging
import sys
from logging.handlers import RotatingFileHandler

from scalping.core.config import LOGS_DIR, LOG_LEVEL, LOG_MAX_BYTES, LOG_BACKUP_COUNT

_initialized = False


def setup_logging():
    """Инициализировать логгер один раз."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)-25s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Консоль
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # Файл (rotating)
    fh = RotatingFileHandler(
        LOGS_DIR / "scalping.log",
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # Заглушить слишком болтливые библиотеки
    for noisy in ["websockets", "aiohttp", "urllib3", "asyncio"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Получить именованный логгер."""
    setup_logging()
    return logging.getLogger(name)
