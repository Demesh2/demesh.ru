import asyncio
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import aiohttp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import websockets

Market = Literal["spot", "futures_usdtm"]


# =========================
# Config
# =========================
@dataclass
class MarketCfg:
    market: Market
    symbol: str
    depth_speed: str = "@100ms"   # futures supports @100ms; spot often supports too, if not set ""
    snapshot_limit: int = 1000


@dataclass
class AppCfg:
    out_dir: Path = Path("./data")
    flush_every_sec: int = 5
    feature_every_sec: int = 1

    # snapshot cadence (strict book also resyncs on gaps; this is just periodic safety + snapshots logging)
    snapshot_every_sec: int = 900  # 15 min

    # Aggression window in seconds (market buys - market sells)
    aggr_window_sec: int = 10

    # Imbalance: top N levels from each side
    imbalance_levels: int = 5

    # How many levels to keep in local maps (memory cap)
    levels_keep: int = 500


# =========================
# Endpoints
# =========================
def endpoints(mcfg: MarketCfg) -> Tuple[str, str]:
    sym = mcfg.symbol.lower()

    if mcfg.market == "spot":
        ws_base = "wss://stream.binance.com:9443"
        rest_base = "https://api.binance.com"
        depth_path = "/api/v3/depth"
    else:
        ws_base = "wss://fstream.binance.com"
        rest_base = "https://fapi.binance.com"
        depth_path = "/fapi/v1/depth"

    streams = [
        f"{sym}@aggTrade",
        f"{sym}@depth{mcfg.depth_speed}",
    ]
    ws_url = f"{ws_base}/stream?streams=" + "/".join(streams)
    snap_url = f"{rest_base}{depth_path}?symbol={mcfg.symbol}&limit={mcfg.snapshot_limit}"
    return ws_url, snap_url


def hour_partition_path(base: Path, market: str, symbol: str, ts_ms: int) -> Path:
    t = time.gmtime(ts_ms / 1000)
    return base / market / symbol / f"date={t.tm_year:04d}-{t.tm_mon:02d}-{t.tm_mday:02d}" / f"hour={t.tm_hour:02d}"


def write_parquet_part(path: Path, prefix: str, rows: List[Dict[str, Any]]):
    """Write rows as new parquet part file for append-friendly storage."""
    if not rows:
        return
    path.mkdir(parents=True, exist_ok=True)
    part = path / f"{prefix}_{int(time.time() * 1000)}.parquet"
    table = pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False)
    pq.write_table(table, part)


# =========================
# Aggression window
# =========================
class AggressionWindow:
    """
    Rolling signed base-qty sum over last window_sec.
    aggTrade has 'm' = isBuyerMaker:
      m==True  => buyer is maker => taker is SELL => signed_qty = -qty
      m==False => taker is BUY  => signed_qty = +qty
    """

    def __init__(self, window_sec: int = 10):
        self.window_ms = window_sec * 1000
        self.events: List[Tuple[int, float]] = []  # (ts_ms, signed_qty)
        self.sum_signed_qty: float = 0.0

    def add(self, ts_ms: int, signed_qty: float):
        self.events.append((ts_ms, signed_qty))
        self.sum_signed_qty += signed_qty
        self._gc(ts_ms)

    def _gc(self, now_ms: int):
        cutoff = now_ms - self.window_ms
        while self.events and self.events[0][0] < cutoff:
            _, old = self.events.pop(0)
            self.sum_signed_qty -= old

    def value(self, now_ms: int) -> float:
        self._gc(now_ms)
        return self.sum_signed_qty


# =========================
# Strict local order book
# =========================
@dataclass
class BookTop:
    bid_p: Optional[float]
    bid_q: Optional[float]
    ask_p: Optional[float]
    ask_q: Optional[float]
    mid: Optional[float]
    spread: Optional[float]
    microprice: Optional[float]
    imbalance: Optional[float]
    last_update_id: Optional[int]
    last_event_ts_ms: Optional[int]


class LocalOrderBookStrict:
    """
    Strictly correct local order book rebuild:
      1) Start WS diff updates, buffer them
      2) Fetch REST snapshot (lastUpdateId)
      3) Drop buffered events with u <= lastUpdateId
      4) Find first event where U <= lastUpdateId+1 <= u, apply it
      5) Then apply every next event requiring U == prev_u + 1
      6) If gap detected -> resync (clear + new snapshot), then replay buffer again

    Works for Spot and Futures as long as diff messages carry U/u and bid/ask deltas.
    """

    def __init__(self, snapshot_url: str, symbol: str, levels_keep: int = 500, top_n_imbalance: int = 5):
        self.snapshot_url = snapshot_url
        self.symbol = symbol
        self.levels_keep = levels_keep
        self.top_n_imbalance = top_n_imbalance

        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}

        self.snapshot_last_update_id: Optional[int] = None
        self.last_update_id: Optional[int] = None  # last applied u
        self.last_event_ts_ms: Optional[int] = None

        self._buffer: List[dict] = []
        self._aligned: bool = False
        self._ready: bool = False

        self._lock = asyncio.Lock()
        self._sess: Optional[aiohttp.ClientSession] = None

    async def _session(self) -> aiohttp.ClientSession:
        if self._sess is None or self._sess.closed:
            self._sess = aiohttp.ClientSession()
        return self._sess

    async def close(self):
        if self._sess and not self._sess.closed:
            await self._sess.close()

    async def bootstrap(self) -> None:
        """Fetch snapshot and prepare to apply diffs. Call once before/around WS start."""
        snap = await self._fetch_snapshot()
        ts_ms = int(time.time() * 1000)
        async with self._lock:
            self._load_snapshot_locked(snap, ts_ms)
            buf = self._buffer[:]
            self._buffer.clear()
        await self._replay_buffer(buf)

    async def resync(self) -> None:
        """Hard resync: snapshot + replay buffered diffs."""
        async with self._lock:
            self._ready = False
            self._aligned = False
            self.bids.clear()
            self.asks.clear()
            self.snapshot_last_update_id = None
            self.last_update_id = None

        snap = await self._fetch_snapshot()
        ts_ms = int(time.time() * 1000)

        async with self._lock:
            self._load_snapshot_locked(snap, ts_ms)
            buf = self._buffer[:]
            self._buffer.clear()

        await self._replay_buffer(buf)

    async def _fetch_snapshot(self) -> dict:
        sess = await self._session()
        async with sess.get(self.snapshot_url, timeout=10) as r:
            return await r.json()

    def _load_snapshot_locked(self, snap: dict, ts_ms: int) -> None:
        self.bids.clear()
        self.asks.clear()

        for p, q in snap.get("bids", []):
            pf = float(p)
            qf = float(q)
            if qf != 0.0:
                self.bids[pf] = qf

        for p, q in snap.get("asks", []):
            pf = float(p)
            qf = float(q)
            if qf != 0.0:
                self.asks[pf] = qf

        sid = int(snap.get("lastUpdateId"))
        self.snapshot_last_update_id = sid
        self.last_update_id = sid  # starts at snapshot id
        self.last_event_ts_ms = ts_ms

        self._aligned = False
        self._ready = True
        self._prune_locked()

    async def on_diff(self, diff: dict) -> bool:
        """
        Apply WS diff strictly.
        diff must contain: U, u, b, a; optional ts_ms.
        Returns True if applied, False if buffered/ignored.
        """
        U = diff.get("U")
        u = diff.get("u")
        if U is None or u is None:
            return False
        U = int(U)
        u = int(u)
        ts_ms = diff.get("ts_ms")

        async with self._lock:
            if not self._ready or self.last_update_id is None:
                self._buffer.append(diff)
                return False

            # Ignore any event fully older than current state
            if u <= self.last_update_id:
                return False

            # Need initial alignment event after snapshot:
            # U <= snapshot_last_update_id+1 <= u
            if not self._aligned:
                target = self.last_update_id + 1  # snapshot id + 1
                if U <= target <= u:
                    self._apply_diff_locked(diff)
                    self.last_update_id = u
                    self._aligned = True
                    if ts_ms is not None:
                        self.last_event_ts_ms = int(ts_ms)
                    self._prune_locked()
                    return True
                else:
                    self._buffer.append(diff)
                    return False

            # After aligned: require continuity U == last_update_id + 1
            expected = self.last_update_id + 1
            if U != expected:
                # gap => resync
                self._buffer.append(diff)
                self._ready = False
                need_resync = True
            else:
                self._apply_diff_locked(diff)
                self.last_update_id = u
                if ts_ms is not None:
                    self.last_event_ts_ms = int(ts_ms)
                self._prune_locked()
                need_resync = False

        if need_resync:
            await self.resync()
            return False
        return True

    def _apply_diff_locked(self, diff: dict) -> None:
        for p, q in diff.get("b", []) or []:
            pf = float(p)
            qf = float(q)
            if qf == 0.0:
                self.bids.pop(pf, None)
            else:
                self.bids[pf] = qf

        for p, q in diff.get("a", []) or []:
            pf = float(p)
            qf = float(q)
            if qf == 0.0:
                self.asks.pop(pf, None)
            else:
                self.asks[pf] = qf

    def _prune_locked(self) -> None:
        """Keep only near-top levels to cap memory."""
        if self.levels_keep <= 0:
            return
        if len(self.bids) > self.levels_keep:
            top_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[: self.levels_keep]
            self.bids = dict(top_bids)
        if len(self.asks) > self.levels_keep:
            top_asks = sorted(self.asks.items(), key=lambda x: x[0])[: self.levels_keep]
            self.asks = dict(top_asks)

    async def _replay_buffer(self, buf: List[dict]) -> None:
        """
        Replay buffered diffs in order. WS can interleave, so sort by u.
        """
        if not buf:
            return
        buf_sorted = sorted(buf, key=lambda d: int(d.get("u", 0)))
        for d in buf_sorted:
            await self.on_diff(d)

    # ---- Feature getters (sync) ----
    def top(self) -> BookTop:
        bid_p, bid_q = self._best_bid()
        ask_p, ask_q = self._best_ask()

        mid = None
        spread = None
        micro = None
        imb = None

        if bid_p is not None and ask_p is not None:
            mid = (bid_p + ask_p) / 2.0
            spread = ask_p - bid_p
            micro = self._microprice(bid_p, bid_q or 0.0, ask_p, ask_q or 0.0)
            imb = self._imbalance(self.top_n_imbalance)

        return BookTop(
            bid_p=bid_p, bid_q=bid_q,
            ask_p=ask_p, ask_q=ask_q,
            mid=mid,
            spread=spread,
            microprice=micro,
            imbalance=imb,
            last_update_id=self.last_update_id,
            last_event_ts_ms=self.last_event_ts_ms,
        )

    def _best_bid(self) -> Tuple[Optional[float], Optional[float]]:
        if not self.bids:
            return None, None
        p = max(self.bids.keys())
        return p, self.bids.get(p)

    def _best_ask(self) -> Tuple[Optional[float], Optional[float]]:
        if not self.asks:
            return None, None
        p = min(self.asks.keys())
        return p, self.asks.get(p)

    @staticmethod
    def _microprice(bid_p: float, bid_q: float, ask_p: float, ask_q: float) -> float:
        denom = bid_q + ask_q
        if denom == 0.0:
            return (bid_p + ask_p) / 2.0
        return (bid_p * ask_q + ask_p * bid_q) / denom

    def _imbalance(self, n: int = 5) -> float:
        if not self.bids or not self.asks:
            return 0.0
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        bid_sum = sum(q for _, q in bids_sorted)
        ask_sum = sum(q for _, q in asks_sorted)
        denom = bid_sum + ask_sum
        if denom == 0.0:
            return 0.0
        return (bid_sum - ask_sum) / denom


# =========================
# Snapshot logger (optional)
# =========================
async def snapshot_logger_loop(
    mcfg: MarketCfg,
    snap_url: str,
    snap_buf: List[Dict[str, Any]],
    stop: asyncio.Event,
    app: AppCfg,
):
    async with aiohttp.ClientSession() as sess:
        while not stop.is_set():
            ts_ms = int(time.time() * 1000)
            try:
                async with sess.get(snap_url, timeout=10) as r:
                    data = await r.json()
                snap_buf.append({
                    "ts_ms": ts_ms,
                    "market": mcfg.market,
                    "symbol": mcfg.symbol,
                    "lastUpdateId": data.get("lastUpdateId"),
                    "bids": data.get("bids"),
                    "asks": data.get("asks"),
                })
            except Exception as e:
                snap_buf.append({
                    "ts_ms": ts_ms,
                    "market": mcfg.market,
                    "symbol": mcfg.symbol,
                    "error": str(e),
                })
            await asyncio.sleep(app.snapshot_every_sec)


# =========================
# WS consumers
# =========================
async def ws_consume(
    mcfg: MarketCfg,
    ws_url: str,
    trades_buf: List[Dict[str, Any]],
    depth_buf: List[Dict[str, Any]],
    book: LocalOrderBookStrict,
    aggr: AggressionWindow,
    stop: asyncio.Event,
):
    while not stop.is_set():
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                async for msg in ws:
                    if stop.is_set():
                        break
                    payload = json.loads(msg)
                    stream = payload.get("stream", "")
                    data = payload.get("data", {})

                    # aggTrade
                    if stream.endswith("@aggTrade"):
                        ts_ms = int(data.get("T"))
                        price = float(data.get("p"))
                        qty = float(data.get("q"))
                        is_buyer_maker = bool(data.get("m"))

                        signed_qty = -qty if is_buyer_maker else qty
                        aggr.add(ts_ms, signed_qty)

                        trades_buf.append({
                            "ts_ms": ts_ms,
                            "market": mcfg.market,
                            "symbol": mcfg.symbol,
                            "price": price,
                            "qty": qty,
                            "isBuyerMaker": is_buyer_maker,
                            "trade_id": data.get("a"),
                        })

                    # depth diff updates
                    elif "@depth" in stream:
                        ts_ms = int(data.get("E"))
                        diff = {
                            "ts_ms": ts_ms,
                            "market": mcfg.market,
                            "symbol": mcfg.symbol,
                            "U": data.get("U"),
                            "u": data.get("u"),
                            "b": data.get("b"),
                            "a": data.get("a"),
                        }
                        depth_buf.append(diff)

                        # strict order book update (resyncs itself on gap)
                        await book.on_diff({
                            "ts_ms": ts_ms,
                            "U": data.get("U"),
                            "u": data.get("u"),
                            "b": data.get("b"),
                            "a": data.get("a"),
                        })

        except Exception:
            await asyncio.sleep(1)


# =========================
# Feature builder (spot as filter)
# =========================
async def feature_builder(
    app: AppCfg,
    spot_book: LocalOrderBookStrict,
    perp_book: LocalOrderBookStrict,
    spot_aggr: AggressionWindow,
    perp_aggr: AggressionWindow,
    feat_buf: List[Dict[str, Any]],
    stop: asyncio.Event,
):
    last_basis: Optional[float] = None

    while not stop.is_set():
        await asyncio.sleep(app.feature_every_sec)
        ts_ms = int(time.time() * 1000)

        s = spot_book.top()
        p = perp_book.top()

        if s.mid is None or p.mid is None:
            continue

        basis = (p.mid - s.mid) / s.mid if s.mid != 0 else 0.0
        basis_change = 0.0 if last_basis is None else (basis - last_basis)
        last_basis = basis

        feat_buf.append({
            "ts_ms": ts_ms,

            # mid prices
            "spot_mid": s.mid,
            "perp_mid": p.mid,

            # basis
            "basis": basis,
            "basis_change": basis_change,

            # spreads
            "spot_spread": s.spread,
            "perp_spread": p.spread,

            # imbalance + microprice
            "spot_imbalance": s.imbalance,
            "perp_imbalance": p.imbalance,
            "spot_microprice": s.microprice,
            "perp_microprice": p.microprice,

            # aggression deltas over rolling window
            "spot_aggr_delta": spot_aggr.value(ts_ms),
            "perp_aggr_delta": perp_aggr.value(ts_ms),

            # book sync ids (debug)
            "spot_last_u": s.last_update_id,
            "perp_last_u": p.last_update_id,
        })


# =========================
# Flusher
# =========================
async def flusher(
    app: AppCfg,
    mcfgs: List[MarketCfg],
    trades_bufs: Dict[Market, List[Dict[str, Any]]],
    depth_bufs: Dict[Market, List[Dict[str, Any]]],
    snap_bufs: Dict[Market, List[Dict[str, Any]]],
    feat_buf: List[Dict[str, Any]],
    stop: asyncio.Event,
):
    while not stop.is_set():
        await asyncio.sleep(app.flush_every_sec)
        now_ms = int(time.time() * 1000)

        # raw + snapshots
        for mcfg in mcfgs:
            market = mcfg.market
            symbol = mcfg.symbol

            for buf, prefix in [
                (trades_bufs[market], "trades"),
                (depth_bufs[market], "depth_updates"),
                (snap_bufs[market], "snapshots"),
            ]:
                if not buf:
                    continue
                rows = buf[:]
                buf.clear()

                ts = rows[0].get("ts_ms") or now_ms
                out_path = hour_partition_path(app.out_dir, market, symbol, int(ts))
                write_parquet_part(out_path, prefix, rows)

        # features
        if feat_buf:
            rows = feat_buf[:]
            feat_buf.clear()
            ts = rows[0].get("ts_ms") or now_ms
            out_path = hour_partition_path(app.out_dir, "features", "SPOT_PERP", int(ts))
            write_parquet_part(out_path, "features_1s", rows)


# =========================
# Main
# =========================
async def main():
    app = AppCfg(
        out_dir=Path("./data"),
        flush_every_sec=5,
        feature_every_sec=1,
        snapshot_every_sec=900,
        aggr_window_sec=10,
        imbalance_levels=5,
        levels_keep=500,
    )

    symbol = "BTCUSDT"

    spot_cfg = MarketCfg(market="spot", symbol=symbol, depth_speed="@100ms", snapshot_limit=1000)
    perp_cfg = MarketCfg(market="futures_usdtm", symbol=symbol, depth_speed="@100ms", snapshot_limit=1000)
    mcfgs = [spot_cfg, perp_cfg]

    # Endpoints
    spot_ws, spot_snap = endpoints(spot_cfg)
    perp_ws, perp_snap = endpoints(perp_cfg)

    # Buffers
    trades_bufs: Dict[Market, List[Dict[str, Any]]] = {"spot": [], "futures_usdtm": []}
    depth_bufs: Dict[Market, List[Dict[str, Any]]] = {"spot": [], "futures_usdtm": []}
    snap_bufs: Dict[Market, List[Dict[str, Any]]] = {"spot": [], "futures_usdtm": []}
    feat_buf: List[Dict[str, Any]] = []

    # Strict books
    spot_book = LocalOrderBookStrict(
        snapshot_url=spot_snap,
        symbol=symbol,
        levels_keep=app.levels_keep,
        top_n_imbalance=app.imbalance_levels,
    )
    perp_book = LocalOrderBookStrict(
        snapshot_url=perp_snap,
        symbol=symbol,
        levels_keep=app.levels_keep,
        top_n_imbalance=app.imbalance_levels,
    )

    # Aggression windows
    spot_aggr = AggressionWindow(window_sec=app.aggr_window_sec)
    perp_aggr = AggressionWindow(window_sec=app.aggr_window_sec)

    stop = asyncio.Event()

    # Bootstrap both books BEFORE starting WS consumers (safe)
    await spot_book.bootstrap()
    await perp_book.bootstrap()

    tasks = [
        # WS consumers
        asyncio.create_task(ws_consume(spot_cfg, spot_ws, trades_bufs["spot"], depth_bufs["spot"], spot_book, spot_aggr, stop)),
        asyncio.create_task(ws_consume(perp_cfg, perp_ws, trades_bufs["futures_usdtm"], depth_bufs["futures_usdtm"], perp_book, perp_aggr, stop)),

        # Snapshot loggers (optional but useful)
        asyncio.create_task(snapshot_logger_loop(spot_cfg, spot_snap, snap_bufs["spot"], stop, app)),
        asyncio.create_task(snapshot_logger_loop(perp_cfg, perp_snap, snap_bufs["futures_usdtm"], stop, app)),

        # Features
        asyncio.create_task(feature_builder(app, spot_book, perp_book, spot_aggr, perp_aggr, feat_buf, stop)),

        # Flusher
        asyncio.create_task(flusher(app, mcfgs, trades_bufs, depth_bufs, snap_bufs, feat_buf, stop)),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        stop.set()
        await asyncio.sleep(0.5)
    finally:
        await spot_book.close()
        await perp_book.close()


if __name__ == "__main__":
    asyncio.run(main())
