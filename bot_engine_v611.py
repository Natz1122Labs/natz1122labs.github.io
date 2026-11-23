
# ============================================================
# NATZ1122 AI AUTO-TRADING BOT
# VERSION 6.1.1 - RECONSTRUCTED ENGINE
# ============================================================
# PAPER TRADING ONLY
# OKX PUBLIC MARKET DATA ONLY
# REAL ORDERS: PERMANENTLY DISABLED
# ============================================================

import os
import json
import time
import math
import copy
import shutil
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone
from collections import deque

import requests

# ============================================================
# 1. SYSTEM IDENTITY
# ============================================================

BOT_NAME = "NATZ1122 AI AUTO-TRADING BOT"
BOT_VERSION = "6.1.1"

WATCHLIST = [
    "BTC-USDT",
    "ETH-USDT",
    "SOL-USDT",
    "XRP-USDT",
    "DOGE-USDT",
    "ADA-USDT",
]

# ============================================================
# 2. ABSOLUTE SAFETY LOCKS
# ============================================================

PAPER_TRADING = True
REAL_TRADING = False
REAL_ORDERS_ENABLED = False

if not PAPER_TRADING:
    raise RuntimeError("Paper trading must remain enabled.")

if REAL_TRADING or REAL_ORDERS_ENABLED:
    raise RuntimeError("REAL TRADING IS PROHIBITED.")

# ============================================================
# 3. MAIN CONFIGURATION
# ============================================================

CONFIG = {
    "exchange": "OKX",
    "timeframe": "1m",

    "starting_balance": 50.0,

    # Simulated exposure controls
    "leverage": 5.0,
    "maximum_leverage": 5.0,
    "margin_fraction": 0.40,
    "maximum_initial_exposure_usdt": 100.0,
    "maximum_open_positions": 1,

    # Signal configuration
    "allow_long": True,
    "allow_short": True,
    "momentum_lookback_seconds": 10.0,
    "minimum_momentum_samples": 2,
    "momentum_threshold": 0.002,

    "rsi_period": 14,
    "rsi_long_ceiling": 70.0,
    "rsi_short_floor": 30.0,

    "use_rsi_filter": True,

    # Market data
    "price_poll_interval": 2.0,
    "candle_refresh_interval": 15.0,
    "whale_refresh_interval": 10.0,
    "scout_interval": 3.0,
    "engine_interval": 1.0,

    "candle_history": 100,
    "large_trade_threshold_usdt": 50000.0,

    # Reliability controls
    "http_timeout": 10,
    "maximum_price_age": 15.0,
    "maximum_signal_age": 8.0,
    "maximum_candle_age": 120.0,
    "maximum_whale_age": 45.0,
    "heartbeat_warning_age": 20.0,

    # Persistence
    "save_interval": 20.0,
    "backup_before_save": True,

    # Network
    "maximum_http_retries": 3,
}

# ============================================================
# 4. RISK CONFIGURATION
# ============================================================

RISK_CONFIG = {
    "minimum_balance": 1.0,
    "maximum_leverage": 5.0,
    "emergency_loss_percent": 10.0,
    "maximum_daily_loss_percent": 5.0,
}

EXIT_CONFIG = {
    "take_profit_percent": 2.0,
    "stop_loss_percent": 0.75,
}

assert EXIT_CONFIG["take_profit_percent"] == 2.0
assert EXIT_CONFIG["stop_loss_percent"] == 0.75

assert CONFIG["leverage"] <= 5.0
assert CONFIG["maximum_open_positions"] == 1
assert len(WATCHLIST) == 6

# ============================================================
# 5. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(BOT_NAME)

# ============================================================
# 6. PERSISTENT STORAGE
# ============================================================

BASE_DIR = Path(
    os.environ.get(
        "NATZ1122_DATA_DIR",
        str(Path(__file__).resolve().parent / "data")
    )
)

BASE_DIR.mkdir(parents=True, exist_ok=True)

BACKUP_DIR = BASE_DIR / "backups"
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

STATE_FILE = BASE_DIR / "v6_state_v611.json"
BACKUP_FILE = BACKUP_DIR / "v6_state_v611_previous.json"
TEMP_STATE_FILE = BASE_DIR / "v6_state_v611.tmp"

# ============================================================
# 7. OKX PUBLIC DATA ENDPOINTS
# ============================================================

OKX_BASE_URL = "https://www.okx.com"

TICKER_ENDPOINT = OKX_BASE_URL + "/api/v5/market/ticker"
CANDLES_ENDPOINT = OKX_BASE_URL + "/api/v5/market/candles"
TRADES_ENDPOINT = OKX_BASE_URL + "/api/v5/market/trades"

HTTP = requests.Session()

HTTP.headers.update({
    "User-Agent": "NATZ1122-PaperTrading-App/6.1.1"
})

# ============================================================
# 8. THREAD MANAGEMENT
# ============================================================

BOT_RUNNING = False
BOT_STOP_EVENT = threading.Event()

BOT_THREAD = None
WORKER_THREADS = {}

STATE_LOCK = threading.RLock()
MARKET_LOCK = threading.RLock()
PERSISTENCE_LOCK = threading.RLock()
LIFECYCLE_LOCK = threading.RLock()

# ============================================================
# 9. MARKET INTELLIGENCE MEMORY
# ============================================================

MARKET_DATA = {
    symbol: {
        "price": None,
        "price_timestamp": None,

        "price_history": deque(maxlen=300),

        "candles": [],
        "candle_timestamp": None,

        "whale": {
            "buy_notional": 0.0,
            "sell_notional": 0.0,
            "net_bias": 0.0,
            "large_trades": 0,
            "timestamp": None,
        },

        "signal": None,
        "score": 0.0,

        "last_error": None,
        "is_top_candidate": False,
    }
    for symbol in WATCHLIST
}

# ============================================================
# 10. ACCOUNT MEMORY
# ============================================================

ACCOUNT = {
    "initial_balance": CONFIG["starting_balance"],
    "balance": CONFIG["starting_balance"],
    "equity": CONFIG["starting_balance"],
    "available_balance": CONFIG["starting_balance"],

    "total_trades": 0,
    "winning_trades": 0,
    "losing_trades": 0,

    "realized_pnl": 0.0,
}

OPEN_POSITION = None
TRADE_HISTORY = []

LAST_SIGNAL = None

# ============================================================
# 11. SYSTEM HEARTBEATS
# ============================================================

WORKER_HEARTBEATS = {
    "price": None,
    "candles": None,
    "whale": None,
    "scout": None,
    "engine": None,
}

LAST_ENGINE_HEARTBEAT = None
LAST_SUPERVISOR_HEARTBEAT = None
LAST_SAVE_TIME = 0.0

OKX_CONNECTED = False
LAST_CONNECTION_ERROR = None

# ============================================================
# END OF PART 1
# ============================================================
# ============================================================
# PART 2 — MARKET INTELLIGENCE
# ============================================================

def valid_price(value):
    """Return True only for a finite, positive numeric price."""
    try:
        number = float(value)
        return math.isfinite(number) and number > 0
    except (TypeError, ValueError):
        return False


def safe_float(value, default=None):
    """Safely convert a value to a finite float."""
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def okx_get(endpoint, params=None):
    """
    Perform a public, read-only OKX GET request.

    Only explicitly permitted public market endpoints are allowed.
    No authentication headers or order endpoints are used.
    """
    allowed_endpoints = {
        OKX_TICKER_ENDPOINT,
        OKX_CANDLES_ENDPOINT,
        OKX_TRADES_ENDPOINT,
    }

    if endpoint not in allowed_endpoints:
        raise ValueError("Blocked non-public or unapproved OKX endpoint.")

    last_error = None

    for attempt in range(CONFIG["max_http_retries"]):
        try:
            response = HTTP_SESSION.get(
                endpoint,
                params=params or {},
                timeout=CONFIG["max_http_timeout"],
            )
            response.raise_for_status()

            payload = response.json()

            if not isinstance(payload, dict):
                raise ValueError("Unexpected OKX response format.")

            if str(payload.get("code", "")) != "0":
                raise ValueError(
                    f"OKX returned error code: {payload.get('code')}"
                )

            data = payload.get("data")
            if not isinstance(data, list):
                raise ValueError("OKX response is missing its data list.")

            return data

        except Exception as exc:
            last_error = exc
            if attempt + 1 < CONFIG["max_http_retries"]:
                time.sleep(min(0.5 * (attempt + 1), 2.0))

    raise RuntimeError(f"OKX public request failed: {last_error}")


def record_market_error(symbol, message):
    """Record a market-data error without terminating a worker."""
    if symbol not in MARKET_DATA:
        return

    with STATE_LOCK:
        MARKET_DATA[symbol]["last_error"] = str(message)[:300]
        MARKET_DATA[symbol]["last_error_time"] = time.time()


def fetch_ticker(symbol):
    """
    Fetch the latest public ticker and append a timestamped price sample.

    Returns the latest valid price, or None if the request/data is invalid.
    """
    if symbol not in MARKET_DATA:
        return None

    try:
        rows = okx_get(
            OKX_TICKER_ENDPOINT,
            params={"instId": symbol},
        )

        if not rows:
            raise ValueError("Ticker response contains no rows.")

        row = rows[0]
        price = safe_float(row.get("last"))

        if not valid_price(price):
            raise ValueError("Ticker contains an invalid last price.")

        exchange_timestamp = safe_float(row.get("ts"))
        received_at = time.time()

        # Keep exchange time for diagnostics, but use local receive time
        # for freshness and elapsed-time calculations.
        sample = {
            "price": float(price),
            "timestamp": received_at,
            "exchange_timestamp_ms": exchange_timestamp,
        }

        with STATE_LOCK:
            market = MARKET_DATA[symbol]
            market["price"] = float(price)
            market["price_timestamp"] = received_at
            market["price_history"].append(sample)
            market["last_error"] = None

        return float(price)

    except Exception as exc:
        record_market_error(symbol, exc)
        LOGGER.warning("Ticker fetch failed for %s: %s", symbol, exc)
        return None


def get_price_snapshot(symbol):
    """Return a consistent copy of the latest price and its timestamp."""
    if symbol not in MARKET_DATA:
        return None

    with STATE_LOCK:
        market = MARKET_DATA[symbol]
        price = market.get("price")
        timestamp = market.get("price_timestamp")

    if not valid_price(price) or not isinstance(timestamp, (int, float)):
        return None

    return {
        "price": float(price),
        "timestamp": float(timestamp),
        "age_seconds": max(0.0, time.time() - float(timestamp)),
    }


def price_is_fresh(symbol):
    """Check whether a symbol has a valid, recent price."""
    snapshot = get_price_snapshot(symbol)

    if snapshot is None:
        return False

    age = snapshot["age_seconds"]
    return 0 <= age <= CONFIG["max_price_age_seconds"]


def fetch_candles(symbol):
    """
    Fetch public OKX candles and store validated OHLCV records.

    OKX returns newest candles first. Records are sorted oldest-to-newest.
    Only confirmed candles are retained for indicator calculations.
    """
    if symbol not in MARKET_DATA:
        return []

    try:
        rows = okx_get(
            OKX_CANDLES_ENDPOINT,
            params={
                "instId": symbol,
                "bar": CONFIG["timeframe"],
                "limit": str(CONFIG["candle_history"]),
            },
        )

        received_at = time.time()
        candles = []

        for row in rows:
            if not isinstance(row, list) or len(row) < 9:
                continue

            timestamp_ms = safe_float(row[0])
            open_price = safe_float(row[1])
            high_price = safe_float(row[2])
            low_price = safe_float(row[3])
            close_price = safe_float(row[4])
            volume = safe_float(row[5], 0.0)
            confirmed = str(row[8]) == "1"

            values = (open_price, high_price, low_price, close_price)

            if timestamp_ms is None:
                continue

            if not all(valid_price(value) for value in values):
                continue

            if high_price < max(open_price, close_price, low_price):
                continue

            if low_price > min(open_price, close_price, high_price):
                continue

            if volume is None or volume < 0:
                continue

            # Ignore the currently forming candle.
            if not confirmed:
                continue

            candles.append({
                "timestamp_ms": int(timestamp_ms),
                "timestamp": timestamp_ms / 1000.0,
                "open": float(open_price),
                "high": float(high_price),
                "low": float(low_price),
                "close": float(close_price),
                "volume": float(volume),
                "confirmed": True,
                "received_at": received_at,
            })

        candles.sort(key=lambda item: item["timestamp_ms"])

        # Remove duplicate timestamps, keeping the latest parsed record.
        unique = {}
        for candle in candles:
            unique[candle["timestamp_ms"]] = candle

        candles = list(unique.values())
        candles.sort(key=lambda item: item["timestamp_ms"])

        with STATE_LOCK:
            market = MARKET_DATA[symbol]
            market["candles"] = candles[-CONFIG["candle_history"]:]
            market["candle_timestamp"] = received_at
            market["last_error"] = None

        return candles

    except Exception as exc:
        record_market_error(symbol, exc)
        LOGGER.warning("Candle fetch failed for %s: %s", symbol, exc)
        return []


def fetch_whale_activity(symbol):
    """
    Estimate large-trade buy/sell flow from OKX public recent trades.

    This is a large-trade activity proxy. It does not identify wallet owners,
    institutions, or whether a trade was opened or closed.
    """
    if symbol not in MARKET_DATA:
        return None

    try:
        rows = okx_get(
            OKX_TRADES_ENDPOINT,
            params={
                "instId": symbol,
                "limit": "100",
            },
        )

        buy_notional = 0.0
        sell_notional = 0.0
        large_trade_count = 0
        threshold = CONFIG["large_trade_threshold_usdt"]

        for row in rows:
            if not isinstance(row, dict):
                continue

            price = safe_float(row.get("px"))
            size = safe_float(row.get("sz"))
            side = str(row.get("side", "")).lower()

            if not valid_price(price) or size is None or size <= 0:
                continue

            notional = price * size

            if notional < threshold:
                continue

            if side == "buy":
                buy_notional += notional
            elif side == "sell":
                sell_notional += notional
            else:
                continue

            large_trade_count += 1

        total = buy_notional + sell_notional

        if total > 0:
            imbalance = (buy_notional - sell_notional) / total
        else:
            imbalance = 0.0

        result = {
            "timestamp": time.time(),
            "buy_notional_usdt": buy_notional,
            "sell_notional_usdt": sell_notional,
            "total_notional_usdt": total,
            "large_trade_count": large_trade_count,
            "imbalance": max(-1.0, min(1.0, imbalance)),
            "source": "OKX public recent trades",
            "is_wallet_identification": False,
        }

        with STATE_LOCK:
            market = MARKET_DATA[symbol]
            market["whale"] = result
            market["whale_timestamp"] = result["timestamp"]
            market["last_error"] = None

        return result

    except Exception as exc:
        record_market_error(symbol, exc)
        LOGGER.warning("Large-trade fetch failed for %s: %s", symbol, exc)
        return None


def calculate_momentum(symbol):
    """
    Calculate price change over an approximate lookback interval.

    Samples are selected by timestamp rather than assuming a fixed polling
    interval. This allows for sequential requests and variable network delays.
    """
    if symbol not in MARKET_DATA:
        return None

    with STATE_LOCK:
        samples = list(MARKET_DATA[symbol]["price_history"])

    if len(samples) < CONFIG["minimum_momentum_samples"]:
        return None

    now = time.time()
    valid_samples = [
        sample for sample in samples
        if valid_price(sample.get("price"))
        and isinstance(sample.get("timestamp"), (int, float))
        and 0 <= now - sample["timestamp"] <=
            CONFIG["max_price_age_seconds"] * 2
    ]

    if len(valid_samples) < CONFIG["minimum_momentum_samples"]:
        return None

    valid_samples.sort(key=lambda item: item["timestamp"])
    latest = valid_samples[-1]
    target_time = latest["timestamp"] - CONFIG["momentum_lookback_seconds"]

    # Use the sample closest to the target time, but reject an interval
    # that is too short or excessively long to be meaningful.
    prior = min(
        valid_samples[:-1],
        key=lambda sample: abs(sample["timestamp"] - target_time),
        default=None,
    )

    if prior is None:
        return None

    elapsed = latest["timestamp"] - prior["timestamp"]

    minimum_elapsed = max(2.0, CONFIG["momentum_lookback_seconds"] * 0.5)
    maximum_elapsed = CONFIG["momentum_lookback_seconds"] * 2.0

    if not minimum_elapsed <= elapsed <= maximum_elapsed:
        return None

    prior_price = float(prior["price"])
    latest_price = float(latest["price"])

    if not valid_price(prior_price):
        return None

    momentum = (latest_price - prior_price) / prior_price

    return {
        "value": momentum,
        "elapsed_seconds": elapsed,
        "start_price": prior_price,
        "end_price": latest_price,
        "timestamp": latest["timestamp"],
    }


def calculate_rsi(symbol, period=None):
    """Calculate simple RSI from confirmed candle closing prices."""
    if symbol not in MARKET_DATA:
        return None

    period = int(period or CONFIG["rsi_period"])

    if period < 2:
        return None

    with STATE_LOCK:
        candles = list(MARKET_DATA[symbol]["candles"])

    if len(candles) < period + 1:
        return None

    closes = [
        safe_float(candle.get("close"))
        for candle in candles[-(period + 1):]
    ]

    if any(not valid_price(close) for close in closes):
        return None

    gains = []
    losses = []

    for previous, current in zip(closes, closes[1:]):
        change = current - previous
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    average_gain = sum(gains) / period
    average_loss = sum(losses) / period

    if average_gain == 0 and average_loss == 0:
        return 50.0

    if average_loss == 0:
        return 100.0

    relative_strength = average_gain / average_loss
    rsi = 100.0 - (100.0 / (1.0 + relative_strength))

    return max(0.0, min(100.0, rsi))


def candle_trend(symbol, candle_count=3):
    """Return a simple trend label from the latest confirmed candles."""
    if symbol not in MARKET_DATA:
        return "UNKNOWN"

    with STATE_LOCK:
        candles = list(MARKET_DATA[symbol]["candles"])

    if len(candles) < candle_count:
        return "UNKNOWN"

    recent = candles[-candle_count:]
    first_close = safe_float(recent[0].get("close"))
    last_close = safe_float(recent[-1].get("close"))

    if not valid_price(first_close) or not valid_price(last_close):
        return "UNKNOWN"

    change = (last_close - first_close) / first_close

    if change > 0.001:
        return "BULLISH"

    if change < -0.001:
        return "BEARISH"

    return "NEUTRAL"


def build_symbol_signal(symbol):
    """
    Build a preliminary signal from current market intelligence.

    This function does not place trades. The Supervisor and Engine must
    validate the result and apply risk controls before paper execution.
    """
    if symbol not in MARKET_DATA:
        return None

    snapshot = get_price_snapshot(symbol)
    if snapshot is None or not price_is_fresh(symbol):
        return None

    with STATE_LOCK:
        market = MARKET_DATA[symbol]
        candles = list(market["candles"])
        whale = copy.deepcopy(market["whale"])
        candle_timestamp = market.get("candle_timestamp")
        whale_timestamp = market.get("whale_timestamp")

    now = time.time()

    if not candles or not isinstance(candle_timestamp, (int, float)):
        return None

    if now - candle_timestamp > CONFIG["max_candle_age_seconds"]:
        return None

    if whale is None or not isinstance(whale_timestamp, (int, float)):
        return None

    if now - whale_timestamp > CONFIG["max_whale_age_seconds"]:
        return None

    momentum_data = calculate_momentum(symbol)
    rsi = calculate_rsi(symbol)
    trend = candle_trend(symbol)

    if momentum_data is None or rsi is None or trend == "UNKNOWN":
        return None

    momentum = momentum_data["value"]
    whale_imbalance = safe_float(whale.get("imbalance"), 0.0)

    if not math.isfinite(momentum) or not math.isfinite(whale_imbalance):
        return None

    # Normalize each component into a bounded score.
    momentum_score = max(-1.0, min(1.0, momentum / 0.01))
    whale_score = max(-1.0, min(1.0, whale_imbalance))

    if trend == "BULLISH":
        candle_score = 1.0
    elif trend == "BEARISH":
        candle_score = -1.0
    else:
        candle_score = 0.0

    if rsi >= CONFIG["rsi_long_ceiling"]:
        rsi_score = -0.5
    elif rsi <= CONFIG["rsi_short_floor"]:
        rsi_score = 0.5
    else:
        rsi_score = 0.0

    combined_score = (
        0.45 * momentum_score
        + 0.30 * candle_score
        + 0.15 * whale_score
        + 0.10 * rsi_score
    )

    if combined_score >= CONFIG["momentum_threshold"] * 100:
        direction = "LONG"
    elif combined_score <= -CONFIG["momentum_threshold"] * 100:
        direction = "SHORT"
    else:
        direction = "NEUTRAL"

    signal = {
        "symbol": symbol,
        "direction": direction,
        "score": combined_score,
        "price": snapshot["price"],
        "price_timestamp": snapshot["timestamp"],
        "signal_timestamp": now,
        "momentum": momentum,
        "momentum_elapsed_seconds": momentum_data["elapsed_seconds"],
        "rsi": rsi,
        "candle_trend": trend,
        "large_trade_imbalance": whale_imbalance,
        "large_trade_count": whale.get("large_trade_count", 0),
        "candle_timestamp": candle_timestamp,
        "whale_timestamp": whale_timestamp,
        "source": "public market data",
    }

    with STATE_LOCK:
        MARKET_DATA[symbol]["signal"] = copy.deepcopy(signal)
        MARKET_DATA[symbol]["score"] = combined_score

    return signal


# ============================================================
# END OF PART 2 — MARKET INTELLIGENCE
# ============================================================
# ============================================================
# PART 3 — WORKER COORDINATION AND SUPERVISOR
# ============================================================

WORKER_STOP_EVENT = threading.Event()
WORKER_THREADS = {}
WORKER_STATUS = {}
SUPERVISOR_STATUS = {
    "timestamp": None,
    "system_reliable": False,
    "reasons": ["Supervisor has not started."],
    "candidate": None,
    "worker_health": {},
}
SUPERVISOR_LOCK = threading.RLock()


def wait_for_worker(interval_seconds):
    """Wait interruptibly so workers can stop promptly."""
    WORKER_STOP_EVENT.wait(max(0.1, float(interval_seconds)))


def update_worker_heartbeat(worker_name, status="OK", detail=None):
    """Record worker health and last successful activity."""
    now = time.time()

    with SUPERVISOR_LOCK:
        WORKER_STATUS[worker_name] = {
            "status": str(status),
            "detail": str(detail)[:300] if detail is not None else None,
            "heartbeat": now,
        }


def worker_is_healthy(worker_name, max_age=None):
    """Check whether a worker has reported a recent heartbeat."""
    max_age = (
        max_age
        if max_age is not None
        else CONFIG["heartbeat_warning_age_seconds"]
    )

    with SUPERVISOR_LOCK:
        state = copy.deepcopy(WORKER_STATUS.get(worker_name))

    if not state or not isinstance(state.get("heartbeat"), (int, float)):
        return False

    age = time.time() - state["heartbeat"]

    return (
        0 <= age <= max_age
        and state.get("status") == "OK"
    )


def run_worker_loop(worker_name, work_function, interval_seconds):
    """
    Run a worker repeatedly without allowing one exception to kill the loop.
    """
    update_worker_heartbeat(worker_name, "OK", "Worker started.")

    while not WORKER_STOP_EVENT.is_set():
        try:
            work_function()
            update_worker_heartbeat(worker_name, "OK")

        except Exception as exc:
            update_worker_heartbeat(worker_name, "ERROR", exc)
            LOGGER.exception("Worker %s encountered an error.", worker_name)

        wait_for_worker(interval_seconds)

    update_worker_heartbeat(worker_name, "STOPPED", "Stop requested.")


def price_worker():
    """Refresh public ticker data for every watched symbol."""
    for symbol in WATCHLIST:
        if WORKER_STOP_EVENT.is_set():
            break

        fetch_ticker(symbol)


def candle_worker():
    """Refresh confirmed candle data for every watched symbol."""
    for symbol in WATCHLIST:
        if WORKER_STOP_EVENT.is_set():
            break

        fetch_candles(symbol)


def whale_intelligence_worker():
    """Refresh public large-trade activity for every watched symbol."""
    for symbol in WATCHLIST:
        if WORKER_STOP_EVENT.is_set():
            break

        fetch_whale_activity(symbol)


def market_scout_worker():
    """
    Build preliminary signals and mark the strongest reliable candidate.

    The Scout only ranks signals. It does not approve or execute trades.
    """
    signal_rows = []

    for symbol in WATCHLIST:
        if WORKER_STOP_EVENT.is_set():
            break

        signal = build_symbol_signal(symbol)

        if not signal:
            with STATE_LOCK:
                MARKET_DATA[symbol]["candidate"] = False
            continue

        if signal.get("direction") not in ("LONG", "SHORT"):
            with STATE_LOCK:
                MARKET_DATA[symbol]["candidate"] = False
            continue

        signal_rows.append(signal)

    if not signal_rows:
        with STATE_LOCK:
            for symbol in WATCHLIST:
                MARKET_DATA[symbol]["candidate"] = False
        return

    # Rank by absolute score; tie-break by freshest signal.
    signal_rows.sort(
        key=lambda row: (
            abs(float(row.get("score", 0.0))),
            float(row.get("signal_timestamp", 0.0)),
        ),
        reverse=True,
    )

    selected = signal_rows[0]
    selected_symbol = selected["symbol"]

    with STATE_LOCK:
        for symbol in WATCHLIST:
            MARKET_DATA[symbol]["candidate"] = (
                symbol == selected_symbol
            )

    with SUPERVISOR_LOCK:
        SUPERVISOR_STATUS["candidate"] = copy.deepcopy(selected)


def get_symbol_intelligence_snapshot(symbol):
    """Return a copied snapshot of a symbol's latest intelligence."""
    if symbol not in MARKET_DATA:
        return None

    with STATE_LOCK:
        market = MARKET_DATA[symbol]

        return {
            "symbol": symbol,
            "price": market.get("price"),
            "price_timestamp": market.get("price_timestamp"),
            "candle_timestamp": market.get("candle_timestamp"),
            "whale_timestamp": market.get("whale_timestamp"),
            "signal": copy.deepcopy(market.get("signal")),
            "score": market.get("score"),
            "candidate": bool(market.get("candidate", False)),
            "last_error": market.get("last_error"),
        }


def validate_symbol_intelligence(symbol):
    """
    Validate required data freshness and signal consistency.

    Returns (is_reliable, reasons, snapshot).
    """
    snapshot = get_symbol_intelligence_snapshot(symbol)

    if snapshot is None:
        return False, ["Unknown symbol."], None

    now = time.time()
    reasons = []

    price = snapshot.get("price")
    price_timestamp = snapshot.get("price_timestamp")
    candle_timestamp = snapshot.get("candle_timestamp")
    whale_timestamp = snapshot.get("whale_timestamp")
    signal = snapshot.get("signal")

    if not valid_price(price):
        reasons.append("Invalid or missing price.")

    if not isinstance(price_timestamp, (int, float)):
        reasons.append("Missing price timestamp.")
    elif now - price_timestamp < 0 or (
        now - price_timestamp > CONFIG["max_price_age_seconds"]
    ):
        reasons.append("Price data is stale.")

    if not isinstance(candle_timestamp, (int, float)):
        reasons.append("Missing candle timestamp.")
    elif now - candle_timestamp < 0 or (
        now - candle_timestamp > CONFIG["max_candle_age_seconds"]
    ):
        reasons.append("Candle data is stale.")

    if not isinstance(whale_timestamp, (int, float)):
        reasons.append("Missing large-trade timestamp.")
    elif now - whale_timestamp < 0 or (
        now - whale_timestamp > CONFIG["max_whale_age_seconds"]
    ):
        reasons.append("Large-trade data is stale.")

    if not isinstance(signal, dict):
        reasons.append("Missing signal.")
    else:
        signal_time = signal.get("signal_timestamp")
        signal_price = signal.get("price")
        signal_direction = signal.get("direction")

        if not isinstance(signal_time, (int, float)):
            reasons.append("Signal timestamp is invalid.")
        elif now - signal_time < 0 or (
            now - signal_time > CONFIG["max_signal_age_seconds"]
        ):
            reasons.append("Signal is stale.")

        if not valid_price(signal_price):
            reasons.append("Signal price is invalid.")

        if signal_direction not in ("LONG", "SHORT", "NEUTRAL"):
            reasons.append("Signal direction is invalid.")

        if valid_price(signal_price) and valid_price(price):
            drift = abs(float(price) - float(signal_price)) / float(signal_price)

            # Reject signals if the market moved materially since scoring.
            if drift > 0.003:
                reasons.append("Price moved more than 0.3% since signal.")

        if signal.get("symbol") != symbol:
            reasons.append("Signal symbol does not match snapshot.")

    return len(reasons) == 0, reasons, snapshot


def supervisor_cycle():
    """
    Consolidate worker health and market intelligence.

    This layer checks reliability and publishes a candidate. The Engine,
    defined in a later part, remains responsible for trading decisions.
    """
    worker_names = (
        "PRICE",
        "CANDLE",
        "WHALE_INTELLIGENCE",
        "MARKET_SCOUT",
    )

    worker_health = {}
    reasons = []

    for worker_name in worker_names:
        healthy = worker_is_healthy(worker_name)
        worker_health[worker_name] = healthy

        if not healthy:
            reasons.append(f"{worker_name} worker is not healthy.")

    reliable_candidates = []

    for symbol in WATCHLIST:
        is_reliable, symbol_reasons, snapshot = (
            validate_symbol_intelligence(symbol)
        )

        if not is_reliable:
            continue

        signal = snapshot.get("signal")

        if (
            isinstance(signal, dict)
            and signal.get("direction") in ("LONG", "SHORT")
        ):
            reliable_candidates.append(signal)

    reliable_candidates.sort(
        key=lambda row: (
            abs(float(row.get("score", 0.0))),
            float(row.get("signal_timestamp", 0.0)),
        ),
        reverse=True,
    )

    candidate = reliable_candidates[0] if reliable_candidates else None

    if candidate is None:
        reasons.append("No reliable directional candidate is available.")

    system_reliable = (
        all(worker_health.values())
        and candidate is not None
    )

    with SUPERVISOR_LOCK:
        SUPERVISOR_STATUS.update({
            "timestamp": time.time(),
            "system_reliable": system_reliable,
            "reasons": reasons,
            "candidate": copy.deepcopy(candidate),
            "worker_health": worker_health,
        })

    return copy.deepcopy(SUPERVISOR_STATUS)


def supervisor_worker():
    """Continuously check worker health and consolidate intelligence."""
    supervisor_cycle()


# ============================================================
# END OF PART 3 — WORKER COORDINATION AND SUPERVISOR
# ============================================================
# ============================================================
# PART 4 — TRADING ENGINE AND RISK DECISIONS
# ============================================================

ENGINE_STATUS = {
    "timestamp": None,
    "decision": None,
    "last_rejection": None,
    "daily_start_balance": None,
    "daily_start_date": None,
    "daily_realized_pnl": 0.0,
    "halted": False,
    "halt_reason": None,
}

ENGINE_LOCK = threading.RLock()


def utc_date_string():
    """Return the current UTC date as YYYY-MM-DD."""
    return datetime.now(timezone.utc).date().isoformat()


def get_account_snapshot():
    """Return a safe copy of the paper account state."""
    with STATE_LOCK:
        return {
            "balance": safe_float(ACCOUNT.get("balance"), 0.0),
            "open_position": copy.deepcopy(OPEN_POSITION),
            "trade_history": copy.deepcopy(TRADE_HISTORY),
        }


def get_open_position():
    """Return a copy of the current paper position, if any."""
    with STATE_LOCK:
        return copy.deepcopy(OPEN_POSITION)


def current_equity():
    """
    Calculate paper-account equity.

    Unrealized PnL is included when a valid open position and fresh price
    are available. Fees, funding, and slippage are not modeled here.
    """
    account = get_account_snapshot()
    balance = account["balance"]
    position = account["open_position"]

    if not isinstance(position, dict):
        return balance

    symbol = position.get("symbol")
    entry_price = safe_float(position.get("entry_price"))
    quantity = safe_float(position.get("quantity"))
    direction = position.get("direction")

    if (
        symbol not in MARKET_DATA
        or not valid_price(entry_price)
        or quantity is None
        or quantity <= 0
        or direction not in ("LONG", "SHORT")
    ):
        return balance

    snapshot = get_price_snapshot(symbol)

    if snapshot is None:
        return balance

    current_price = snapshot["price"]

    if direction == "LONG":
        unrealized_pnl = (current_price - entry_price) * quantity
    else:
        unrealized_pnl = (entry_price - current_price) * quantity

    return balance + unrealized_pnl


def get_used_margin(position=None):
    """Return the margin reserved for the current paper position."""
    if position is None:
        position = get_open_position()

    if not isinstance(position, dict):
        return 0.0

    margin = safe_float(position.get("margin"), 0.0)
    return max(0.0, margin)


def get_position_notional(position=None):
    """Return the position's opening notional value."""
    if position is None:
        position = get_open_position()

    if not isinstance(position, dict):
        return 0.0

    notional = safe_float(position.get("notional"), 0.0)
    return max(0.0, notional)


def initialize_daily_risk_state():
    """Initialize or roll the daily risk tracking date."""
    today = utc_date_string()

    with ENGINE_LOCK:
        if ENGINE_STATUS.get("daily_start_date") != today:
            balance = get_account_snapshot()["balance"]

            ENGINE_STATUS["daily_start_date"] = today
            ENGINE_STATUS["daily_start_balance"] = balance
            ENGINE_STATUS["daily_realized_pnl"] = 0.0

            # A new UTC day resets the daily-loss halt, but not a
            # separate system-safety halt.
            if ENGINE_STATUS.get("halt_reason") == "DAILY_LOSS_LIMIT":
                ENGINE_STATUS["halted"] = False
                ENGINE_STATUS["halt_reason"] = None


def record_realized_pnl(pnl):
    """Add a closed paper trade's PnL to the daily risk total."""
    pnl = safe_float(pnl)

    if pnl is None:
        return

    initialize_daily_risk_state()

    with ENGINE_LOCK:
        ENGINE_STATUS["daily_realized_pnl"] += pnl


def daily_loss_limit_reached():
    """Check whether realized daily loss has reached its configured limit."""
    initialize_daily_risk_state()

    with ENGINE_LOCK:
        start_balance = safe_float(
            ENGINE_STATUS.get("daily_start_balance"),
            0.0,
        )
        realized_pnl = safe_float(
            ENGINE_STATUS.get("daily_realized_pnl"),
            0.0,
        )

    if start_balance <= 0:
        return True

    max_daily_loss = start_balance * (
        RISK_CONFIG["maximum_daily_loss_pct"] / 100.0
    )

    return realized_pnl <= -max_daily_loss


def emergency_margin_loss_reached(position=None):
    """
    Check whether unrealized loss has reached the emergency margin limit.

    The normal stop loss should generally close the position first.
    This is an additional safeguard.
    """
    if position is None:
        position = get_open_position()

    if not isinstance(position, dict):
        return False

    symbol = position.get("symbol")
    entry_price = safe_float(position.get("entry_price"))
    quantity = safe_float(position.get("quantity"))
    direction = position.get("direction")
    margin = safe_float(position.get("margin"))

    if (
        symbol not in MARKET_DATA
        or not valid_price(entry_price)
        or quantity is None
        or quantity <= 0
        or margin is None
        or margin <= 0
        or direction not in ("LONG", "SHORT")
    ):
        return True

    snapshot = get_price_snapshot(symbol)

    if snapshot is None:
        return False

    current_price = snapshot["price"]

    if direction == "LONG":
        unrealized_pnl = (current_price - entry_price) * quantity
    else:
        unrealized_pnl = (entry_price - current_price) * quantity

    emergency_loss = margin * (
        RISK_CONFIG["emergency_loss_pct_used_margin"] / 100.0
    )

    return unrealized_pnl <= -emergency_loss


def calculate_position_size(symbol, entry_price):
    """
    Calculate a paper position using configured balance and exposure limits.

    Uses the configured margin fraction, leverage cap, and maximum notional.
    Returns a sizing dictionary or None if sizing is not safe.
    """
    if symbol not in WATCHLIST or not valid_price(entry_price):
        return None

    account = get_account_snapshot()
    balance = account["balance"]

    if not math.isfinite(balance) or balance < RISK_CONFIG["minimum_balance"]:
        return None

    if account["open_position"] is not None:
        return None

    configured_leverage = safe_float(CONFIG.get("leverage"), 1.0)
    maximum_leverage = safe_float(
        RISK_CONFIG.get("maximum_leverage"),
        1.0,
    )

    if (
        configured_leverage is None
        or maximum_leverage is None
        or configured_leverage <= 0
        or maximum_leverage <= 0
    ):
        return None

    leverage = min(configured_leverage, maximum_leverage)

    margin_fraction = safe_float(CONFIG.get("margin_fraction"), 0.0)

    if margin_fraction is None or not 0 < margin_fraction <= 1:
        return None

    margin = balance * margin_fraction
    max_exposure = safe_float(CONFIG.get("max_exposure_usdt"), 0.0)

    if max_exposure is None or max_exposure <= 0:
        return None

    notional = min(margin * leverage, max_exposure)

    if notional <= 0 or margin <= 0:
        return None

    quantity = notional / float(entry_price)

    if not math.isfinite(quantity) or quantity <= 0:
        return None

    return {
        "symbol": symbol,
        "entry_price": float(entry_price),
        "quantity": quantity,
        "notional": notional,
        "margin": margin,
        "leverage": leverage,
    }


def validate_candidate_for_entry(candidate):
    """
    Validate a Supervisor candidate before allowing a paper entry decision.

    Returns (approved, reasons, sizing).
    """
    reasons = []

    if not isinstance(candidate, dict):
        return False, ["No candidate supplied."], None

    symbol = candidate.get("symbol")
    direction = candidate.get("direction")
    signal_timestamp = candidate.get("signal_timestamp")
    signal_price = candidate.get("price")

    if symbol not in WATCHLIST:
        reasons.append("Candidate symbol is not on the watchlist.")

    if direction not in ("LONG", "SHORT"):
        reasons.append("Candidate has no directional signal.")

    if not isinstance(signal_timestamp, (int, float)):
        reasons.append("Candidate timestamp is invalid.")
    else:
        age = time.time() - signal_timestamp

        if age < 0 or age > CONFIG["max_signal_age_seconds"]:
            reasons.append("Candidate signal is stale.")

    if not valid_price(signal_price):
        reasons.append("Candidate price is invalid.")

    if ENGINE_STATUS.get("halted"):
        reasons.append("Engine is halted.")

    account = get_account_snapshot()

    if account["open_position"] is not None:
        reasons.append("A paper position is already open.")

    if account["balance"] < RISK_CONFIG["minimum_balance"]:
        reasons.append("Paper balance is below the minimum.")

    if daily_loss_limit_reached():
        reasons.append("Daily realized loss limit reached.")

    if symbol in WATCHLIST:
        reliable, reliability_reasons, snapshot = (
            validate_symbol_intelligence(symbol)
        )

        if not reliable:
            reasons.extend(reliability_reasons)

        if snapshot and valid_price(signal_price):
            current_price = safe_float(snapshot.get("price"))

            if valid_price(current_price):
                drift = abs(current_price - signal_price) / signal_price

                if drift > 0.003:
                    reasons.append(
                        "Candidate price differs from current price by over 0.3%."
                    )

    sizing = None

    if not reasons:
        sizing = calculate_position_size(symbol, signal_price)

        if sizing is None:
            reasons.append("Position sizing failed risk validation.")

    return len(reasons) == 0, reasons, sizing


def engine_decision_cycle():
    """
    Make a risk-checked decision from the Supervisor's latest candidate.

    This function never places an order. It only returns a decision record.
    """
    initialize_daily_risk_state()

    with SUPERVISOR_LOCK:
        supervisor = copy.deepcopy(SUPERVISOR_STATUS)

    if not supervisor.get("system_reliable"):
        decision = {
            "action": "NO_TRADE",
            "reason": "Supervisor reports unreliable intelligence.",
            "timestamp": time.time(),
        }

        with ENGINE_LOCK:
            ENGINE_STATUS["timestamp"] = decision["timestamp"]
            ENGINE_STATUS["decision"] = copy.deepcopy(decision)
            ENGINE_STATUS["last_rejection"] = decision["reason"]

        return decision

    candidate = supervisor.get("candidate")
    approved, reasons, sizing = validate_candidate_for_entry(candidate)

    if not approved:
        decision = {
            "action": "NO_TRADE",
            "reason": "; ".join(reasons),
            "candidate": copy.deepcopy(candidate),
            "timestamp": time.time(),
        }

        with ENGINE_LOCK:
            ENGINE_STATUS["timestamp"] = decision["timestamp"]
            ENGINE_STATUS["decision"] = copy.deepcopy(decision)
            ENGINE_STATUS["last_rejection"] = decision["reason"]

        return decision

    decision = {
        "action": "PAPER_ENTRY_CANDIDATE",
        "symbol": candidate["symbol"],
        "direction": candidate["direction"],
        "score": candidate.get("score"),
        "entry_price": sizing["entry_price"],
        "quantity": sizing["quantity"],
        "notional": sizing["notional"],
        "margin": sizing["margin"],
        "leverage": sizing["leverage"],
        "take_profit_pct": EXIT_CONFIG["take_profit_pct"],
        "stop_loss_pct": EXIT_CONFIG["stop_loss_pct"],
        "timestamp": time.time(),
        "paper_only": True,
        "real_orders_enabled": False,
    }

    with ENGINE_LOCK:
        ENGINE_STATUS["timestamp"] = decision["timestamp"]
        ENGINE_STATUS["decision"] = copy.deepcopy(decision)
        ENGINE_STATUS["last_rejection"] = None

    return decision


def engine_worker():
    """Run the decision cycle repeatedly without executing trades."""
    engine_decision_cycle()


# ============================================================
# END OF PART 4 — TRADING ENGINE AND RISK DECISIONS
# ============================================================

"""
NATZ1122 AI TRADING BOT V6.1.1
PART 5 OF 8 — RISK MANAGEMENT

Paper trading only.
Real exchange orders are permanently blocked in this module.
"""

from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
import math


@dataclass
class RiskConfig:
    starting_balance: float = 50.0
    take_profit_pct: float = 2.0
    stop_loss_pct: float = 0.75

    risk_per_trade_pct: float = 1.0
    max_exposure: float = 100.0
    max_leverage: float = 5.0
    max_open_positions: int = 1

    paper_trading_only: bool = True
    real_orders_enabled: bool = False


@dataclass
class RiskDecision:
    approved: bool
    reason: str
    symbol: str = ""
    side: str = ""
    entry_price: float = 0.0
    quantity: float = 0.0
    notional: float = 0.0
    margin_required: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0

    def to_dict(self):
        return asdict(self)


class RiskManagerV611:

    def __init__(self, config=None):
        self.config = config or RiskConfig()

        self.balance = self.config.starting_balance
        self.open_positions = {}

        self.enabled = True

        # Hard safety checks
        if not self.config.paper_trading_only:
            raise ValueError("LIVE TRADING IS NOT SUPPORTED")

        if self.config.real_orders_enabled:
            raise ValueError("REAL ORDERS MUST REMAIN DISABLED")

        if self.config.take_profit_pct != 2.0:
            raise ValueError("V6.1.1 TP must be 2.0%")

        if self.config.stop_loss_pct != 0.75:
            raise ValueError("V6.1.1 SL must be 0.75%")

    @staticmethod
    def valid_number(value):
        try:
            return math.isfinite(float(value)) and float(value) > 0
        except (TypeError, ValueError):
            return False

    def approve_entry(
        self,
        symbol: str,
        side: str,
        entry_price: float,
        available_balance: Optional[float] = None
    ) -> RiskDecision:

        side = str(side).lower()

        if not self.enabled:
            return RiskDecision(False, "Risk manager disabled")

        if not symbol:
            return RiskDecision(False, "Invalid trading symbol")

        if side not in ("long", "short"):
            return RiskDecision(False, "Invalid trading direction")

        if not self.valid_number(entry_price):
            return RiskDecision(False, "Invalid entry price")

        entry_price = float(entry_price)

        if symbol in self.open_positions:
            return RiskDecision(False, "Position already exists")

        if len(self.open_positions) >= self.config.max_open_positions:
            return RiskDecision(False, "Maximum open positions reached")

        balance = (
            self.balance
            if available_balance is None
            else float(available_balance)
        )

        if not self.valid_number(balance):
            return RiskDecision(False, "Invalid available balance")

        # Risk budget: maximum 1% of available balance
        risk_amount = balance * (
            self.config.risk_per_trade_pct / 100
        )

        stop_distance = (
            entry_price * self.config.stop_loss_pct / 100
        )

        if stop_distance <= 0:
            return RiskDecision(False, "Invalid stop distance")

        # Position sizing based on maximum acceptable loss
        risk_quantity = risk_amount / stop_distance

        # Exposure protection
        exposure_quantity = (
            self.config.max_exposure / entry_price
        )

        quantity = min(risk_quantity, exposure_quantity)

        notional = quantity * entry_price

        # Simulated leverage protection
        leverage = min(self.config.max_leverage, 5.0)

        margin_required = notional / leverage

        if notional > self.config.max_exposure:
            return RiskDecision(False, "Maximum exposure exceeded")

        if margin_required > balance:
            return RiskDecision(False, "Insufficient paper margin")

        if quantity <= 0 or not math.isfinite(quantity):
            return RiskDecision(False, "Invalid position quantity")

        # Long and short protective price calculations
        if side == "long":
            stop_loss = entry_price * (
                1 - self.config.stop_loss_pct / 100
            )

            take_profit = entry_price * (
                1 + self.config.take_profit_pct / 100
            )

        else:
            stop_loss = entry_price * (
                1 + self.config.stop_loss_pct / 100
            )

            take_profit = entry_price * (
                1 - self.config.take_profit_pct / 100
            )

        return RiskDecision(
            approved=True,
            reason="Risk checks passed - PAPER TRADE ONLY",
            symbol=symbol,
            side=side,
            entry_price=entry_price,
            quantity=quantity,
            notional=notional,
            margin_required=margin_required,
            stop_loss=stop_loss,
            take_profit=take_profit
        )

    def register_position(self, decision: RiskDecision):

        if not isinstance(decision, RiskDecision):
            return False

        if not decision.approved:
            return False

        if not self.enabled:
            return False

        if not self.config.paper_trading_only:
            return False

        if self.config.real_orders_enabled:
            return False

        if decision.symbol in self.open_positions:
            return False

        if len(self.open_positions) >= self.config.max_open_positions:
            return False

        self.open_positions[decision.symbol] = {
            "symbol": decision.symbol,
            "side": decision.side,
            "entry_price": decision.entry_price,
            "quantity": decision.quantity,
            "notional": decision.notional,
            "margin_required": decision.margin_required,
            "stop_loss": decision.stop_loss,
            "take_profit": decision.take_profit,
            "status": "PAPER_OPEN"
        }

        return True

    def check_exit(self, symbol: str, current_price: float):

        position = self.open_positions.get(symbol)

        if position is None:
            return {
                "exit": False,
                "reason": "No open position"
            }

        if not self.valid_number(current_price):
            return {
                "exit": False,
                "reason": "Invalid market price"
            }

        current_price = float(current_price)

        side = position["side"]

        if side == "long":
            if current_price <= position["stop_loss"]:
                return {
                    "exit": True,
                    "reason": "STOP_LOSS",
                    "exit_price": position["stop_loss"]
                }

            if current_price >= position["take_profit"]:
                return {
                    "exit": True,
                    "reason": "TAKE_PROFIT",
                    "exit_price": position["take_profit"]
                }

        elif side == "short":
            if current_price >= position["stop_loss"]:
                return {
                    "exit": True,
                    "reason": "STOP_LOSS",
                    "exit_price": position["stop_loss"]
                }

            if current_price <= position["take_profit"]:
                return {
                    "exit": True,
                    "reason": "TAKE_PROFIT",
                    "exit_price": position["take_profit"]
                }

        return {
            "exit": False,
            "reason": "POSITION_HOLDING"
        }

    def close_position(
        self,
        symbol: str,
        exit_price: float
    ):

        position = self.open_positions.get(symbol)

        if position is None:
            return {
                "success": False,
                "reason": "Position not found"
            }

        if not self.valid_number(exit_price):
            return {
                "success": False,
                "reason": "Invalid exit price"
            }

        entry = position["entry_price"]
        quantity = position["quantity"]

        if position["side"] == "long":
            pnl = (float(exit_price) - entry) * quantity
        else:
            pnl = (entry - float(exit_price)) * quantity

        self.balance += pnl

        result = {
            "success": True,
            "symbol": symbol,
            "side": position["side"],
            "entry_price": entry,
            "exit_price": float(exit_price),
            "quantity": quantity,
            "realized_pnl": pnl,
            "paper_balance": self.balance,
            "status": "PAPER_CLOSED"
        }

        del self.open_positions[symbol]

        return result

    def emergency_stop(self):

        self.enabled = False

        return {
            "success": True,
            "status": "EMERGENCY_STOP",
            "new_entries_allowed": False,
            "open_positions": len(self.open_positions),
            "message": "New paper entries blocked. "
                       "Existing positions require exit management."
        }

    def resume(self):

        self.enabled = True

        return {
            "success": True,
            "status": "RISK_MANAGER_ACTIVE"
        }

    def get_status(self):

        return {
            "version": "V6.1.1",
            "module": "RISK_MANAGER",
            "balance": self.balance,
            "open_positions": len(self.open_positions),
            "max_open_positions": self.config.max_open_positions,
            "take_profit_pct": self.config.take_profit_pct,
            "stop_loss_pct": self.config.stop_loss_pct,
            "max_exposure": self.config.max_exposure,
            "max_leverage": self.config.max_leverage,
            "paper_trading_only": True,
            "real_orders_enabled": False,
            "risk_manager_enabled": self.enabled
        }
# ============================================================
# END OF PART 5 OF 8
# NATZ1122 AI TRADING BOT V6.1.1
# ============================================================

# ============================================================
# PART 6 OF 8
# SUPERVISOR - INTELLIGENCE CONSOLIDATION & SYSTEM MONITORING
# NATZ1122 AI TRADING BOT V6.1.1
# ============================================================

SUPERVISOR_APPROVED_SIGNALS = {}
SUPERVISOR_STATUS = "INITIALIZING"
SUPERVISOR_REPORT = {}
SUPERVISOR_LOCK = threading.RLock()

MAX_SIGNAL_AGE = 15.0
MAX_CANDLE_AGE = 45.0
MAX_WHALE_AGE = 30.0


def supervisor_validate_worker_data(symbol):
    """Validate independent worker information before approval."""

    if symbol not in WATCHLIST:
        return False, "UNKNOWN_SYMBOL"

    now = time.time()

    with MARKET_LOCK:
        data = copy.deepcopy(MARKET_DATA[symbol])

    # PRICE validation
    price = data.get("price")
    price_stamp = data.get("price_timestamp")

    if not valid_price(price) or price_stamp is None:
        return False, "PRICE_UNAVAILABLE"

    if not (0 <= now - price_stamp <= CONFIG["maximum_price_age"]):
        return False, "STALE_PRICE"

    # CANDLE validation
    candle_stamp = data.get("candle_timestamp")

    if candle_stamp is None:
        return False, "CANDLE_DATA_UNAVAILABLE"

    if not (0 <= now - candle_stamp <= MAX_CANDLE_AGE):
        return False, "STALE_CANDLE_DATA"

    candles = data.get("candles", [])

    confirmed = [
        candle for candle in candles
        if candle.get("confirmed") is True
    ]

    if len(confirmed) < CONFIG["rsi_period"] + 1:
        return False, "INSUFFICIENT_CANDLE_DATA"

    # WHALE INTELLIGENCE validation
    whale = data.get("whale", {})
    whale_stamp = whale.get("timestamp")

    if whale_stamp is None:
        return False, "WHALE_DATA_UNAVAILABLE"

    if not (0 <= now - whale_stamp <= MAX_WHALE_AGE):
        return False, "STALE_WHALE_DATA"

    # MARKET SCOUT validation
    signal = data.get("signal")

    if not isinstance(signal, dict):
        return False, "NO_QUALIFIED_SIGNAL"

    signal_stamp = signal.get("timestamp")

    if signal_stamp is None:
        return False, "SIGNAL_TIMESTAMP_MISSING"

    if not (0 <= now - signal_stamp <= MAX_SIGNAL_AGE):
        return False, "STALE_SIGNAL"

    if signal.get("symbol") != symbol:
        return False, "SIGNAL_SYMBOL_MISMATCH"

    if signal.get("direction") not in ("LONG", "SHORT"):
        return False, "INVALID_DIRECTION"

    if not valid_price(signal.get("price")):
        return False, "INVALID_SIGNAL_PRICE"

    if not math.isfinite(float(signal.get("score", 0))):
        return False, "INVALID_SIGNAL_SCORE"

    return True, "VALIDATED"


def supervisor_collect_intelligence():
    """
    Consolidate intelligence from all six monitored coins.
    Only validated signals can reach the ENGINE.
    """

    global SUPERVISOR_APPROVED_SIGNALS
    global SUPERVISOR_REPORT

    approved = {}
    rejected = {}

    for symbol in WATCHLIST:

        valid, reason = supervisor_validate_worker_data(symbol)

        if valid:

            with MARKET_LOCK:
                signal = copy.deepcopy(
                    MARKET_DATA[symbol]["signal"]
                )

            approved[symbol] = signal

        else:
            rejected[symbol] = reason

    # Highest-scoring qualified candidate comes first.
    ranked = sorted(
        approved.values(),
        key=lambda item: item["score"],
        reverse=True
    )

    with SUPERVISOR_LOCK:

        SUPERVISOR_APPROVED_SIGNALS = {
            item["symbol"]: item
            for item in ranked
        }

        SUPERVISOR_REPORT = {
            "timestamp": time.time(),
            "approved_count": len(approved),
            "rejected_count": len(rejected),
            "approved_symbols": list(approved.keys()),
            "rejected_symbols": rejected,
            "best_candidate": (
                ranked[0] if ranked else None
            )
        }

    return ranked


def supervisor_check_worker_health():
    """Monitor independent worker heartbeats."""

    now = time.time()
    health = {}

    for worker in (
        "price",
        "candles",
        "whale",
        "scout"
    ):

        heartbeat = WORKER_HEARTBEATS.get(worker)

        if heartbeat is None:
            health[worker] = False

        else:
            age = now - heartbeat

            health[worker] = (
                0 <= age <= CONFIG["heartbeat_warning_age"]
            )

    health["engine"] = (
        BOT_THREAD is not None
        and BOT_THREAD.is_alive()
    )

    return health


def supervisor_safety_check():
    """Stop new entries when intelligence is unreliable."""

    global SUPERVISOR_STATUS

    health = supervisor_check_worker_health()

    required_workers = (
        "price",
        "candles",
        "whale",
        "scout"
    )

    failed = [
        worker
        for worker in required_workers
        if not health.get(worker, False)
    ]

    if failed:

        SUPERVISOR_STATUS = "WARNING"

        logger.warning(
            "SUPERVISOR: unreliable workers: %s",
            ", ".join(failed)
        )

        with SUPERVISOR_LOCK:
            SUPERVISOR_APPROVED_SIGNALS = {}

        return False

    SUPERVISOR_STATUS = "HEALTHY"

    return True


def supervisor_cycle():
    """One complete intelligence consolidation cycle."""

    if not supervisor_safety_check():
        return []

    candidates = supervisor_collect_intelligence()

    logger.info(
        "SUPERVISOR | approved=%s | rejected=%s",
        len(candidates),
        len(SUPERVISOR_REPORT.get("rejected_symbols", {}))
    )

    return candidates


def supervisor_intelligence_status():

    with SUPERVISOR_LOCK:

        print("\nNATZ1122 SUPERVISOR STATUS")
        print("-" * 60)

        print("Status:", SUPERVISOR_STATUS)

        print(
            "Approved signals:",
            SUPERVISOR_REPORT.get("approved_count", 0)
        )

        print(
            "Rejected signals:",
            SUPERVISOR_REPORT.get("rejected_count", 0)
        )

        print(
            "Approved symbols:",
            list(SUPERVISOR_APPROVED_SIGNALS.keys())
        )

        print(
            "Rejected symbols:",
            SUPERVISOR_REPORT.get("rejected_symbols", {})
        )

        print("-" * 60)


# ============================================================
# END OF PART 6 OF 8
# ============================================================

# ============================================================
# 10. ENGINE AND SUPERVISOR - V6.1.1 INTEGRATED
# ============================================================

def engine_worker():
    global LAST_ENGINE_HEARTBEAT

    logger.info("ENGINE worker started.")

    while not BOT_STOP_EVENT.is_set():
        try:
            LAST_ENGINE_HEARTBEAT = time.time()
            WORKER_HEARTBEATS["engine"] = LAST_ENGINE_HEARTBEAT

            # Existing position management always has priority.
            if has_open_position():
                manage_open_position()

            elif account_is_healthy():

                # ENGINE accepts only signals approved by SUPERVISOR.
                with SUPERVISOR_LOCK:
                    approved = copy.deepcopy(
                        SUPERVISOR_APPROVED_SIGNALS
                    )

                candidates = []

                for symbol, signal in approved.items():
                    if symbol not in WATCHLIST:
                        continue

                    if not isinstance(signal, dict):
                        continue

                    if signal.get("symbol") != symbol:
                        continue

                    direction = signal.get("direction")
                    if direction not in ("LONG", "SHORT"):
                        continue

                    if direction == "LONG" and not CONFIG["allow_long"]:
                        continue

                    if direction == "SHORT" and not CONFIG["allow_short"]:
                        continue

                    if not price_is_fresh(symbol):
                        continue

                    if not valid_price(signal.get("price")):
                        continue

                    try:
                        score = float(signal.get("score", 0))
                    except (TypeError, ValueError):
                        continue

                    if not math.isfinite(score) or score <= 0:
                        continue

                    candidates.append(signal)

                # Highest-scoring SUPERVISOR-approved signal wins.
                if candidates:
                    candidates.sort(
                        key=lambda item: float(item["score"]),
                        reverse=True
                    )

                    best_signal = candidates[0]

                    logger.info(
                        "ENGINE selected approved signal: %s %s | Score: %.4f",
                        best_signal["symbol"],
                        best_signal["direction"],
                        float(best_signal["score"])
                    )

                    # Paper execution only.
                    open_paper_position(best_signal)

            # Periodic paper-account persistence.
            if time.time() - LAST_SAVE_TIME >= CONFIG["save_interval"]:
                save_state()

        except Exception:
            logger.exception(
                "ENGINE cycle error; continuing safely."
            )

        BOT_STOP_EVENT.wait(CONFIG["engine_interval"])

    logger.info("ENGINE worker stopped.")


# Register the integrated ENGINE.
WORKERS = {
    "price": price_worker,
    "candles": candle_worker,
    "whale": whale_worker,
    "scout": market_scout_worker,
    "engine": engine_worker,
}


def _worker_entry(name, target):
    try:
        target()
    except Exception:
        logger.exception(
            "%s worker terminated unexpectedly.",
            name
        )


def trading_supervisor():
    global BOT_RUNNING
    global WORKER_THREADS
    global LAST_SUPERVISOR_HEARTBEAT
    global SUPERVISOR_STATUS
    global SUPERVISOR_REPORT
    global SUPERVISOR_APPROVED_SIGNALS

    threads = {}

    try:
        # Launch independent intelligence workers and ENGINE.
        for name, target in WORKERS.items():
            thread = threading.Thread(
                target=_worker_entry,
                args=(name, target),
                name="NATZ1122_V611_" + name.upper(),
                daemon=True
            )

            threads[name] = thread
            thread.start()

        WORKER_THREADS = threads

        logger.info(
            "SUPERVISOR active; all V6.1.1 workers launched."
        )

        while not BOT_STOP_EVENT.wait(2.0):

            LAST_SUPERVISOR_HEARTBEAT = time.time()

            # SUPERVISOR consolidates and validates intelligence.
            try:
                supervisor_cycle()

            except Exception as error:
                logger.exception(
                    "SUPERVISOR cycle failed."
                )

                # Fail closed: do not allow stale approvals.
                with SUPERVISOR_LOCK:
                    SUPERVISOR_APPROVED_SIGNALS = {}

                SUPERVISOR_STATUS = "SAFETY_HOLD"
                SUPERVISOR_REPORT = {
                    "status": "SAFETY_HOLD",
                    "error": str(error),
                    "timestamp": time.time()
                }

            # Restart workers that unexpectedly stop.
            for name, target in WORKERS.items():

                thread = threads.get(name)

                if thread is None or not thread.is_alive():

                    if BOT_STOP_EVENT.is_set():
                        break

                    logger.error(
                        "%s worker exited; attempting restart.",
                        name
                    )

                    replacement = threading.Thread(
                        target=_worker_entry,
                        args=(name, target),
                        name=(
                            "NATZ1122_V611_"
                            + name.upper()
                            + "_RESTART"
                        ),
                        daemon=True
                    )

                    threads[name] = replacement
                    replacement.start()

            WORKER_THREADS = threads

            # Open-position stale-price warning.
            position = position_snapshot()

            if position and not price_is_fresh(
                position["symbol"]
            ):
                logger.warning(
                    "SUPERVISOR: open-position market price is stale."
                )

    finally:
        BOT_STOP_EVENT.set()

        # Remove outstanding approvals during shutdown.
        with SUPERVISOR_LOCK:
            SUPERVISOR_APPROVED_SIGNALS = {}

        for thread in threads.values():
            if (
                thread.is_alive()
                and thread is not threading.current_thread()
            ):
                thread.join(timeout=5.0)

        save_state()

        BOT_RUNNING = False
        SUPERVISOR_STATUS = "STOPPED"

        logger.info("SUPERVISOR stopped.")

        display_position_status()
        display_trade_statistics()


# ============================================================
# END OF SECTION 10 - V6.1.1 INTEGRATED ENGINE
# ============================================================
# ============================================================
# END OF PART 7 OF 8
# ============================================================

# ============================================================
# 13. FINAL INTEGRATION AND SYSTEM READINESS - V6.1.1
# ============================================================

def final_system_check():
    """
    Final V6.1.1 integration check.
    This function checks configuration and required components.
    It does not start the bot or submit any orders.
    """

    print("\n" + "=" * 68)
    print("NATZ1122 AI TRADING BOT V6.1.1")
    print("FINAL SYSTEM READINESS CHECK")
    print("=" * 68)

    checks = {}

    # --------------------------------------------------------
    # PAPER-TRADING SAFETY
    # --------------------------------------------------------

    checks["Paper trading enabled"] = (
        PAPER_TRADING is True
        and CONFIG.get("paper_trading") is True
    )

    checks["Real trading disabled"] = (
        REAL_TRADING is False
        and CONFIG.get("real_trading") is False
    )

    checks["Real orders disabled"] = (
        REAL_ORDERS_ENABLED is False
        and CONFIG.get("allow_real_orders") is False
    )

    # --------------------------------------------------------
    # CONFIGURATION
    # --------------------------------------------------------

    checks["Starting balance is 50 USDT"] = (
        CONFIG.get("starting_balance") == 50.0
    )

    checks["Leverage capped at 5X"] = (
        CONFIG.get("leverage") == 5.0
        and RISK_CONFIG.get("maximum_leverage") == 5.0
    )

    checks["Maximum exposure is 100 USDT"] = (
        CONFIG.get("maximum_initial_exposure_usdt") == 100.0
    )

    checks["Maximum one open position"] = (
        CONFIG.get("maximum_open_positions") == 1
    )

    checks["Take profit is 2%"] = (
        EXIT_CONFIG.get("take_profit_percent") == 2.0
    )

    checks["Stop loss is 0.75%"] = (
        EXIT_CONFIG.get("stop_loss_percent") == 0.75
    )

    checks["Six-coin watchlist configured"] = (
        len(WATCHLIST) == 6
        and len(set(WATCHLIST)) == 6
    )

    # --------------------------------------------------------
    # REQUIRED ENGINE COMPONENTS
    # --------------------------------------------------------

    required_functions = (
        "initialize_bot",
        "start_bot",
        "stop_bot",
        "trading_supervisor",
        "engine_worker",
        "supervisor_cycle",
        "supervisor_safety_check",
        "market_scout_worker",
        "price_worker",
        "candle_worker",
        "whale_worker",
        "open_paper_position",
        "close_paper_position",
        "save_state",
        "load_state",
        "display_account_report",
    )

    missing_functions = [
        name for name in required_functions
        if not callable(globals().get(name))
    ]

    checks["Required engine functions available"] = (
        len(missing_functions) == 0
    )

    # --------------------------------------------------------
    # WORKER CONFIGURATION
    # --------------------------------------------------------

    expected_workers = {
        "price",
        "candles",
        "whale",
        "scout",
        "engine",
    }

    checks["Five independent workers configured"] = (
        set(WORKERS.keys()) == expected_workers
    )

    checks["Worker heartbeat tracking available"] = (
        set(WORKER_HEARTBEATS.keys()) == expected_workers
    )

    # --------------------------------------------------------
    # PUBLIC MARKET DATA ONLY
    # --------------------------------------------------------

    checks["Public OKX market-data endpoints configured"] = (
        OKX_BASE_URL == "https://www.okx.com"
        and TICKER_ENDPOINT.startswith(
            OKX_BASE_URL + "/api/v5/market/"
        )
        and CANDLES_ENDPOINT.startswith(
            OKX_BASE_URL + "/api/v5/market/"
        )
        and TRADES_ENDPOINT.startswith(
            OKX_BASE_URL + "/api/v5/market/"
        )
    )

    # --------------------------------------------------------
    # DISPLAY RESULTS
    # --------------------------------------------------------

    for label, passed in checks.items():
        print(
            ("PASS" if passed else "FAIL")
            + " - "
            + label
        )

    if missing_functions:
        print("\nMissing functions:")
        for name in missing_functions:
            print(" -", name)

    print("-" * 68)

    all_passed = all(checks.values())

    if all_passed:
        print("ALL FINAL READINESS CHECKS PASSED.")
        print("V6.1.1 IS CONFIGURED FOR PAPER TRADING ONLY.")
    else:
        print("READINESS CHECK FAILED.")
        print("DO NOT START THE BOT UNTIL THE FAILURES ARE FIXED.")

    print("=" * 68)

    return all_passed


# ============================================================
# CONSOLIDATED SYSTEM DIAGNOSTICS
# ============================================================

def system_diagnostic_report():
    """
    Displays the final system configuration and runtime status.
    No trading action is performed.
    """

    print("\n" + "=" * 68)
    print("NATZ1122 V6.1.1 - SYSTEM DIAGNOSTIC REPORT")
    print("=" * 68)

    print("Bot version:", BOT_VERSION)
    print("Bot running:", BOT_RUNNING)
    print("Supervisor alive:", (
        BOT_THREAD.is_alive() if BOT_THREAD else False
    ))

    print("\nTRADING SAFETY")
    print("Paper trading:", PAPER_TRADING)
    print("Real trading:", REAL_TRADING)
    print("Real orders:", REAL_ORDERS_ENABLED)

    print("\nRISK SETTINGS")
    print("Starting balance:", CONFIG["starting_balance"], "USDT")
    print("Simulated leverage:", CONFIG["leverage"], "X")
    print(
        "Maximum exposure:",
        CONFIG["maximum_initial_exposure_usdt"],
        "USDT"
    )
    print("Maximum open positions:", CONFIG["maximum_open_positions"])
    print("Take profit:", EXIT_CONFIG["take_profit_percent"], "%")
    print("Stop loss:", EXIT_CONFIG["stop_loss_percent"], "%")

    print("\nMARKET WATCHLIST")
    for symbol in WATCHLIST:
        print(" -", symbol)

    print("\nSUPERVISOR")
    if "SUPERVISOR_STATUS" in globals():
        print("Status:", SUPERVISOR_STATUS)
    if "SUPERVISOR_REPORT" in globals():
        print("Latest report:", SUPERVISOR_REPORT)

    print("\nWORKER STATUS")
    for name in WORKERS:
        thread = WORKER_THREADS.get(name)
        alive = thread.is_alive() if thread else False
        print("{}: {}".format(name, "RUNNING" if alive else "NOT RUNNING"))

    print("\nSTORAGE")
    print("Data directory:", BASE_DIR)
    print("State file:", STATE_FILE)
    print("State file exists:", STATE_FILE.exists())

    print("=" * 68)

    return True


# ============================================================
# FINAL INITIALIZATION HELPER
# ============================================================

def prepare_v611():
    """
    Validates and initializes the simulated account.
    The bot remains stopped until start_bot() is called.
    """

    print("\nPreparing NATZ1122 V6.1.1...")

    if not final_system_check():
        raise RuntimeError(
            "V6.1.1 final readiness check failed. "
            "Bot startup blocked."
        )

    initialize_bot()

    print("\nV6.1.1 PREPARATION COMPLETE.")
    print("Bot is initialized but NOT running.")
    print("Use start_bot() to start paper trading.")
    print("Use stop_bot() to stop the supervisor.")

    return True


# ============================================================
# END OF PART 8 OF 8
# ============================================================
