#!/usr/bin/env python3
"""
Binance ML Scalping MVP — единственная точка входа.

Использование:
    python run.py              — запустить live систему
    python run.py train        — обучить модель (offline)
    python run.py label        — разметить собранные данные
    python run.py label+train  — разметить + обучить
"""

import sys
import os
from pathlib import Path

# ─── Гарантируем корректный sys.path для Windows ─────────────────
# Добавляем корень проекта (папку где лежит этот файл) в sys.path,
# чтобы "python run.py" работал из любой директории.
PROJECT_ROOT = str(Path(__file__).resolve().parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def main():
    import asyncio

    command = sys.argv[1] if len(sys.argv) > 1 else "live"

    if command == "live":
        _run_live()
    elif command == "train":
        _run_train()
    elif command == "label":
        _run_label()
    elif command == "label+train":
        _run_label()
        _run_train()
    else:
        print(f"Неизвестная команда: {command}")
        print("Использование:")
        print("  python run.py              — live система")
        print("  python run.py train        — обучить модель")
        print("  python run.py label        — разметить данные")
        print("  python run.py label+train  — разметить + обучить")
        sys.exit(1)


def _run_live():
    """Запустить live систему (сбор данных + features + inference + web)."""
    import asyncio
    from scalping.orchestrator import main as orchestrator_main

    print("=" * 60)
    print("  BINANCE ML SCALPING MVP")
    print("  Starting live system...")
    print("=" * 60)

    # На Windows используем WindowsSelectorEventLoopPolicy для совместимости
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(orchestrator_main())
    except KeyboardInterrupt:
        print("\nStopped by user (Ctrl+C)")


def _run_train():
    """Обучить модели офлайн."""
    from scalping.core.logger import setup_logging
    setup_logging()

    print("Training models...")
    from scalping.training.trainer import train_all
    train_all()
    print("Training complete!")


def _run_label():
    """Разметить собранные данные."""
    from scalping.core.logger import setup_logging
    setup_logging()
    from scalping.core.config import SYMBOLS

    print("Labeling features...")
    from scalping.labeling.labeler import label_all_features
    for symbol in SYMBOLS:
        label_all_features(symbol)
    print("Labeling complete!")


if __name__ == "__main__":
    main()
