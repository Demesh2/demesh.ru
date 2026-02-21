"""
Orchestrator — запускает все компоненты в одном asyncio event loop.

Компоненты:
1. WebSocket коллекторы (Futures + Spot) для каждого символа
2. Depth manager для каждого символа
3. Feature loop
4. Live inference loop
5. Paper trader
6. FastAPI web server (uvicorn)
"""

import asyncio
import signal
import sys

import uvicorn

from scalping.core.logger import get_logger, setup_logging
from scalping.core.state import SharedState
from scalping.core.config import SYMBOLS, WEB_HOST, WEB_PORT

from scalping.data_collector.futures_ws import (
    FuturesBookTickerWS,
    FuturesAggTradesWS,
    FuturesKlineWS,
)
from scalping.data_collector.spot_ws import (
    SpotBookTickerWS,
    SpotAggTradesWS,
    SpotKlineWS,
)
from scalping.data_collector.depth_manager import DepthManager
from scalping.features.loop import FeatureLoop
from scalping.inference.live_loop import LiveInferenceLoop
from scalping.paper_trading.trader import PaperTrader
from scalping.web.app import app, set_state

log = get_logger("orchestrator")


async def run_uvicorn(state: SharedState):
    """Запустить uvicorn внутри asyncio loop."""
    config = uvicorn.Config(
        app=app,
        host=WEB_HOST,
        port=WEB_PORT,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)

    # Отключаем установку signal handlers uvicorn'ом — мы управляем сами
    server.install_signal_handlers = lambda: None

    log.info("Web panel starting on http://%s:%d", WEB_HOST, WEB_PORT)
    await server.serve()


async def main():
    """Главная корутина — запускает все компоненты."""
    setup_logging()
    log.info("=" * 60)
    log.info("Binance ML Scalping MVP starting...")
    log.info("Symbols: %s", SYMBOLS)
    log.info("=" * 60)

    state = SharedState()
    set_state(state)

    # Регистрируем graceful shutdown
    loop = asyncio.get_event_loop()

    def _shutdown_handler():
        log.info("Shutdown signal received, stopping...")
        state.shutdown_event.set()

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown_handler)
    else:
        # На Windows signal handlers в asyncio не работают напрямую
        # Используем threading для Ctrl+C
        import threading

        def _win_signal_handler(signum, frame):
            _shutdown_handler()

        signal.signal(signal.SIGINT, _win_signal_handler)
        signal.signal(signal.SIGTERM, _win_signal_handler)

    # Собираем все задачи
    tasks = []

    # WebSocket коллекторы для каждого символа
    for symbol in SYMBOLS:
        tasks.append(asyncio.create_task(
            FuturesBookTickerWS(symbol, state).run(),
            name=f"fut_bt_{symbol}",
        ))
        tasks.append(asyncio.create_task(
            FuturesAggTradesWS(symbol, state).run(),
            name=f"fut_at_{symbol}",
        ))
        tasks.append(asyncio.create_task(
            FuturesKlineWS(symbol, state).run(),
            name=f"fut_kl_{symbol}",
        ))
        tasks.append(asyncio.create_task(
            SpotBookTickerWS(symbol, state).run(),
            name=f"spot_bt_{symbol}",
        ))
        tasks.append(asyncio.create_task(
            SpotAggTradesWS(symbol, state).run(),
            name=f"spot_at_{symbol}",
        ))
        tasks.append(asyncio.create_task(
            SpotKlineWS(symbol, state).run(),
            name=f"spot_kl_{symbol}",
        ))
        tasks.append(asyncio.create_task(
            DepthManager(symbol, state).run(),
            name=f"depth_{symbol}",
        ))

    log.info("Started %d WebSocket tasks for %d symbols", len(tasks), len(SYMBOLS))

    # Feature loop (ждём 3 секунды, чтобы собрать первые данные)
    async def delayed_feature_loop():
        await asyncio.sleep(3)
        await FeatureLoop(state).run()

    tasks.append(asyncio.create_task(delayed_feature_loop(), name="feature_loop"))

    # Live inference (ждём 5 секунд)
    async def delayed_inference():
        await asyncio.sleep(5)
        await LiveInferenceLoop(state).run()

    tasks.append(asyncio.create_task(delayed_inference(), name="inference_loop"))

    # Paper trader (ждём 6 секунд)
    async def delayed_paper_trader():
        await asyncio.sleep(6)
        await PaperTrader(state).run()

    tasks.append(asyncio.create_task(delayed_paper_trader(), name="paper_trader"))

    # Web server
    tasks.append(asyncio.create_task(run_uvicorn(state), name="web_server"))

    log.info("All %d tasks started. Web panel: http://%s:%d", len(tasks), WEB_HOST, WEB_PORT)

    # Ждём shutdown или завершения любой задачи с ошибкой
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        for task in done:
            if task.exception():
                log.error("Task %s failed: %s", task.get_name(), task.exception())

    except asyncio.CancelledError:
        pass
    finally:
        # Graceful shutdown
        state.shutdown_event.set()
        log.info("Shutting down, cancelling %d tasks...", len(tasks))

        for task in tasks:
            if not task.done():
                task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        log.info("All tasks stopped. Goodbye!")
