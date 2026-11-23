# ============================================================
# NATZ1122 AI TRADING BOT - V6.1.1
# COMPLETE STANDALONE PAPER-TRADING ENGINE
#
# Multi-coin public-market scanner:
# BTC-USDT, ETH-USDT, SOL-USDT, XRP-USDT, DOGE-USDT, ADA-USDT
#
# Four-layer architecture:
# 1) Independent intelligence workers: MARKET SCOUT, WHALE,
#    CANDLE and PRICE
# 2) SUPERVISOR: worker lifecycle and safety monitoring
# 3) ENGINE: signal validation, portfolio and risk decisions
# 4) EXECUTION: simulated paper entries/exits only
#
# SAFETY:
# - OKX PUBLIC market-data endpoints only
# - No API keys, authenticated endpoints, or order endpoints
# - No real orders can be submitted by this module
# - One open position maximum; total exposure capped at 100 USDT
# - Paper results exclude fees, slippage, funding and liquidation
# - Persistent storage requires a persistent mounted volume
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
# 1. IDENTITY AND CONFIGURATION
# ============================================================

BOT_NAME = "NATZ1122 AI TRADING BOT"
BOT_VERSION = "6.1.1"

WATCHLIST = [
    "BTC-USDT",
    "ETH-USDT",
    "SOL-USDT",
    "XRP-USDT",
    "DOGE-USDT",
    "ADA-USDT",
]

CONFIG = {
    "exchange": "OKX",
    "watchlist": WATCHLIST,
    "timeframe": "1m",
    "allow_long": True,
    "allow_short": True,

    "starting_balance": 50.0,
    "leverage": 5.0,
    "margin_fraction": 1.0,
    "maximum_initial_exposure_usdt": 100.0,
    "maximum_open_positions": 1,

    "momentum_threshold": 0.002,  # Percent, not decimal fraction
    "momentum_interval": 3.0,
    "momentum_tolerance": 1.25,

    "take_profit_percent": 2.0,   # Underlying coin price movement
    "stop_loss_percent": 0.75,    # Underlying coin price movement

    "use_rsi_filter": True,
    "rsi_period": 14,
    "rsi_long_ceiling": 70.0,
    "rsi_short_floor": 30.0,

    "price_poll_interval": 3.0,
    "candle_refresh_interval": 15.0,
    "whale_refresh_interval": 10.0,
    "scout_interval": 3.0,
    "engine_interval": 1.0,
    "candle_history": 100,
    "large_trade_threshold_usdt": 50000.0,

    "http_timeout": 10,
    "maximum_price_age": 15.0,
    "reconnect_interval": 3.0,
    "save_interval": 20.0,
    "heartbeat_warning_age": 20.0,

    "paper_trading": True,
    "real_trading": False,
    "allow_real_orders": False,

    "backup_before_save": True,
    "log_level": "INFO",
}

PAPER_TRADING = True
REAL_TRADING = False
REAL_ORDERS_ENABLED = False

RISK_CONFIG = {
    "minimum_balance": 1.0,
    "maximum_leverage": 5.0,
    "emergency_loss_percent": 10.0,  # Percent of used margin
    "reject_invalid_prices": True,
}

EXIT_CONFIG = {
    "take_profit_percent": 2.0,
    "stop_loss_percent": 0.75,
}

assert PAPER_TRADING is True
assert REAL_TRADING is False
assert REAL_ORDERS_ENABLED is False
assert CONFIG["paper_trading"] is True
assert CONFIG["real_trading"] is False
assert CONFIG["allow_real_orders"] is False

# ============================================================
# 2. LOGGING, STORAGE AND PUBLIC HTTP SESSION
# ============================================================

logging.basicConfig(
    level=getattr(logging, CONFIG["log_level"]),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger(BOT_NAME)

# On Render, set NATZ1122_DATA_DIR to the mount path of a persistent disk.
# Without a persistent disk, local files may be lost on redeploy/restart.
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

OKX_BASE_URL = "https://www.okx.com"
TICKER_ENDPOINT = OKX_BASE_URL + "/api/v5/market/ticker"
CANDLES_ENDPOINT = OKX_BASE_URL + "/api/v5/market/candles"
TRADES_ENDPOINT = OKX_BASE_URL + "/api/v5/market/trades"

HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "NATZ1122-PaperTrading-App/6.1.1"})

# ============================================================
# 3. THREADS, LOCKS AND RUNTIME DATA
# ============================================================

BOT_RUNNING = False
BOT_STOP_EVENT = threading.Event()

BOT_THREAD = None
WORKER_THREADS = {}

STATE_LOCK = threading.RLock()
LIFECYCLE_LOCK = threading.RLock()
MARKET_LOCK = threading.RLock()
PERSISTENCE_LOCK = threading.RLock()

MARKET_DATA = {
    symbol: {
        "price": None,
        "price_timestamp": None,
        "price_history": deque(maxlen=100),
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
    }
    for symbol in WATCHLIST
}

LAST_SIGNAL = None
LAST_ENGINE_HEARTBEAT = None
LAST_SUPERVISOR_HEARTBEAT = None
LAST_SAVE_TIME = 0.0
LAST_CONNECTION_ERROR = None

OPEN_POSITION = None
TRADE_HISTORY = []

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

WORKER_HEARTBEATS = {
    "price": None,
    "candles": None,
    "whale": None,
    "scout": None,
    "engine": None,
}

OKX_CONNECTED = False

# ============================================================
# 4. VALIDATION AND SAFETY
# ============================================================

def validate_configuration():
    if not WATCHLIST or len(set(WATCHLIST)) != len(WATCHLIST):
        raise RuntimeError("Watchlist is empty or contains duplicates.")
    if CONFIG["starting_balance"] <= 0:
        raise RuntimeError("Starting balance must be positive.")
    if CONFIG["leverage"] <= 0 or CONFIG["leverage"] > RISK_CONFIG["maximum_leverage"]:
        raise RuntimeError("Configured leverage exceeds the safety limit.")
    if CONFIG["maximum_initial_exposure_usdt"] <= 0:
        raise RuntimeError("Maximum exposure must be positive.")
    if CONFIG["maximum_open_positions"] != 1:
        raise RuntimeError("V6.1.1 safety policy requires one open position.")
    if EXIT_CONFIG["take_profit_percent"] != 2.0:
        raise RuntimeError("V6.1.1 take-profit setting mismatch.")
    if EXIT_CONFIG["stop_loss_percent"] != 0.75:
        raise RuntimeError("V6.1.1 stop-loss setting mismatch.")
    if not (PAPER_TRADING and not REAL_TRADING and not REAL_ORDERS_ENABLED):
        raise RuntimeError("Paper-trading safety lock failed.")
    logger.info("V6.1.1 configuration validation passed.")
    return True


def verify_paper_trading_safety():
    checks = {
        "Paper trading enabled": PAPER_TRADING is True,
        "Real trading disabled": REAL_TRADING is False,
        "Real orders disabled": REAL_ORDERS_ENABLED is False,
        "No authenticated trading API configured": True,
        "Six-coin watchlist configured": len(WATCHLIST) == 6,
        "One open position maximum": CONFIG["maximum_open_positions"] == 1,
        "Simulated leverage capped at 5X": CONFIG["leverage"] == 5.0,
        "Maximum exposure capped at 100 USDT":
            CONFIG["maximum_initial_exposure_usdt"] == 100.0,
        "Take profit set to 2%": EXIT_CONFIG["take_profit_percent"] == 2.0,
        "Stop loss set to 0.75%": EXIT_CONFIG["stop_loss_percent"] == 0.75,
        "Only public market-data endpoints":
            all(path in (TICKER_ENDPOINT, CANDLES_ENDPOINT, TRADES_ENDPOINT)
                for path in (TICKER_ENDPOINT, CANDLES_ENDPOINT, TRADES_ENDPOINT)),
    }

    print("\nV6.1.1 SAFETY VERIFICATION")
    print("-" * 58)
    for label, passed in checks.items():
        print(("PASS" if passed else "FAIL"), "-", label)
    if not all(checks.values()):
        raise RuntimeError("Safety verification failed.")
    print("-" * 58)
    print("ALL SAFETY CHECKS PASSED - PAPER TRADING ONLY.")
    return True


def valid_price(value):
    try:
        number = float(value)
        return math.isfinite(number) and number > 0
    except (TypeError, ValueError):
        return False


def price_is_fresh(symbol):
    with MARKET_LOCK:
        item = MARKET_DATA.get(symbol)
        if not item:
            return False
        price = item["price"]
        stamp = item["price_timestamp"]
    if price is None or stamp is None:
        return False
    age = time.time() - stamp
    return 0 <= age <= CONFIG["maximum_price_age"]


def get_price(symbol):
    with MARKET_LOCK:
        item = MARKET_DATA.get(symbol)
        return None if item is None else item["price"]


# ============================================================
# 5. STATE PERSISTENCE - V6.1.1 ISOLATED STATE
# ============================================================

def build_state():
    with STATE_LOCK:
        return {
            "version": BOT_VERSION,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "account": copy.deepcopy(ACCOUNT),
            "open_position": copy.deepcopy(OPEN_POSITION),
            "trade_history": copy.deepcopy(TRADE_HISTORY),
        }


def validate_saved_state(data):
    if not isinstance(data, dict) or data.get("version") != BOT_VERSION:
        raise ValueError("State file is not a V6.1.1 state.")
    account = data.get("account")
    if not isinstance(account, dict):
        raise ValueError("Missing account data.")

    required = (
        "initial_balance", "balance", "equity", "available_balance",
        "total_trades", "winning_trades", "losing_trades", "realized_pnl",
    )
    for key in required:
        value = account.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("Invalid account field: " + key)
        if not math.isfinite(value):
            raise ValueError("Non-finite account field: " + key)

    if account["initial_balance"] <= 0 or account["balance"] < 0:
        raise ValueError("Invalid account balance.")
    for key in ("total_trades", "winning_trades", "losing_trades"):
        if not isinstance(account[key], int) or account[key] < 0:
            raise ValueError("Invalid trade counter: " + key)
    if account["winning_trades"] + account["losing_trades"] > account["total_trades"]:
        raise ValueError("Inconsistent trade counters.")

    position = data.get("open_position")
    if position is not None:
        if not isinstance(position, dict):
            raise ValueError("Invalid open position.")
        required_position = (
            "symbol", "direction", "entry_price", "quantity",
            "notional_value", "margin_used", "leverage", "entry_time", "status",
        )
        for key in required_position:
            if key not in position:
                raise ValueError("Missing position field: " + key)
        if position["symbol"] not in WATCHLIST:
            raise ValueError("Saved position symbol is not in V6.1.1 watchlist.")
        if position["direction"] not in ("LONG", "SHORT") or position["status"] != "OPEN":
            raise ValueError("Invalid saved position direction/status.")
        for key in ("entry_price", "quantity", "notional_value", "margin_used", "leverage", "entry_time"):
            value = position[key]
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError("Invalid saved position value: " + key)
            if not math.isfinite(value) or value <= 0:
                raise ValueError("Invalid saved position value: " + key)
        if position["notional_value"] > CONFIG["maximum_initial_exposure_usdt"] + 1e-8:
            raise ValueError("Saved position exceeds maximum exposure.")

    history = data.get("trade_history", [])
    if not isinstance(history, list):
        raise ValueError("Invalid trade history.")
    return True


def save_state():
    global LAST_SAVE_TIME
    if not PAPER_TRADING or REAL_TRADING or REAL_ORDERS_ENABLED:
        raise RuntimeError("Persistence safety violation.")

    with PERSISTENCE_LOCK:
        data = build_state()
        try:
            with open(TEMP_STATE_FILE, "w", encoding="utf-8") as handle:
                json.dump(data, handle, indent=2, allow_nan=False)
                handle.flush()
                os.fsync(handle.fileno())

            if STATE_FILE.exists() and CONFIG["backup_before_save"]:
                shutil.copy2(STATE_FILE, BACKUP_FILE)
            os.replace(str(TEMP_STATE_FILE), str(STATE_FILE))
            LAST_SAVE_TIME = time.time()
            return True
        except Exception:
            try:
                if TEMP_STATE_FILE.exists():
                    TEMP_STATE_FILE.unlink()
            except OSError:
                pass
            logger.exception("V6.1.1 state save failed.")
            return False


def load_state():
    global OPEN_POSITION, TRADE_HISTORY

    if not STATE_FILE.exists():
        with STATE_LOCK:
            ACCOUNT.update({
                "initial_balance": CONFIG["starting_balance"],
                "balance": CONFIG["starting_balance"],
                "equity": CONFIG["starting_balance"],
                "available_balance": CONFIG["starting_balance"],
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "realized_pnl": 0.0,
            })
            OPEN_POSITION = None
            TRADE_HISTORY = []
        logger.info("Fresh V6.1.1 paper account created: 50 USDT.")
        save_state()
        return False

    with open(STATE_FILE, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    validate_saved_state(data)

    with STATE_LOCK:
        for key in ACCOUNT:
            ACCOUNT[key] = data["account"][key]
        OPEN_POSITION = copy.deepcopy(data.get("open_position"))
        TRADE_HISTORY = copy.deepcopy(data.get("trade_history", []))

    logger.info("V6.1.1 paper account restored.")
    if OPEN_POSITION:
        logger.warning(
            "Restored open paper position: %s %s",
            OPEN_POSITION["direction"], OPEN_POSITION["symbol"]
        )
    return True


# ============================================================
# 6. OKX PUBLIC MARKET DATA
# ============================================================

def okx_get(endpoint, params):
    response = HTTP.get(endpoint, params=params, timeout=CONFIG["http_timeout"])
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") != "0":
        raise RuntimeError(payload.get("msg", "OKX public API error."))
    return payload.get("data", [])


def fetch_ticker(symbol):
    global OKX_CONNECTED, LAST_CONNECTION_ERROR
    data = okx_get(TICKER_ENDPOINT, {"instId": symbol})
    if not data:
        raise RuntimeError("No ticker returned for " + symbol)
    price = float(data[0]["last"])
    if not valid_price(price):
        raise ValueError("Invalid ticker price for " + symbol)
    stamp = time.time()
    with MARKET_LOCK:
        MARKET_DATA[symbol]["price"] = price
        MARKET_DATA[symbol]["price_timestamp"] = stamp
        MARKET_DATA[symbol]["price_history"].append(
            {"timestamp": stamp, "price": price}
        )
        MARKET_DATA[symbol]["last_error"] = None
    OKX_CONNECTED = True
    LAST_CONNECTION_ERROR = None
    return price


def fetch_candles(symbol):
    raw = okx_get(
        CANDLES_ENDPOINT,
        {
            "instId": symbol,
            "bar": CONFIG["timeframe"],
            "limit": str(CONFIG["candle_history"]),
        },
    )
    candles = []
    for row in reversed(raw):
        try:
            candle = {
                "timestamp": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "confirmed": str(row[8]) == "1",
            }
            values = (candle["open"], candle["high"], candle["low"], candle["close"])
            if not all(valid_price(value) for value in values):
                continue
            if candle["high"] < max(candle["open"], candle["close"]):
                continue
            if candle["low"] > min(candle["open"], candle["close"]):
                continue
            if candle["high"] < candle["low"]:
                continue
            candles.append(candle)
        except (IndexError, TypeError, ValueError):
            continue
    if not candles:
        raise RuntimeError("No valid candles for " + symbol)
    with MARKET_LOCK:
        MARKET_DATA[symbol]["candles"] = candles
        MARKET_DATA[symbol]["candle_timestamp"] = time.time()
    return candles


def fetch_whale_activity(symbol):
    # Public recent-trades feed only. This is large-trade flow estimation,
    # NOT identification of wallet owners or proof of institutional activity.
    raw = okx_get(TRADES_ENDPOINT, {"instId": symbol, "limit": "100"})
    buy_value = 0.0
    sell_value = 0.0
    large_count = 0

    for trade in raw:
        try:
            price = float(trade["px"])
            size = float(trade["sz"])
            notional = price * size
            if not valid_price(price) or not math.isfinite(size) or size <= 0:
                continue
            if notional >= CONFIG["large_trade_threshold_usdt"]:
                large_count += 1
                if trade.get("side") == "buy":
                    buy_value += notional
                elif trade.get("side") == "sell":
                    sell_value += notional
        except (KeyError, TypeError, ValueError):
            continue

    total = buy_value + sell_value
    bias = 0.0 if total <= 0 else (buy_value - sell_value) / total
    result = {
        "buy_notional": buy_value,
        "sell_notional": sell_value,
        "net_bias": bias,
        "large_trades": large_count,
        "timestamp": time.time(),
    }
    with MARKET_LOCK:
        MARKET_DATA[symbol]["whale"] = result
    return result


# ============================================================
# 7. MARKET INDICATORS AND SCORING
# ============================================================

def calculate_momentum(symbol):
    with MARKET_LOCK:
        samples = list(MARKET_DATA[symbol]["price_history"])
    if len(samples) < 2:
        return None

    current = samples[-1]
    target = current["timestamp"] - CONFIG["momentum_interval"]
    previous = min(samples[:-1], key=lambda item: abs(item["timestamp"] - target))
    elapsed = current["timestamp"] - previous["timestamp"]
    if elapsed <= 0 or abs(elapsed - CONFIG["momentum_interval"]) > CONFIG["momentum_tolerance"]:
        return None
    return ((current["price"] - previous["price"]) / previous["price"]) * 100.0


def calculate_rsi(symbol):
    with MARKET_LOCK:
        candles = copy.deepcopy(MARKET_DATA[symbol]["candles"])
    closes = [c["close"] for c in candles if c["confirmed"]]
    period = CONFIG["rsi_period"]
    if len(closes) < period + 1:
        return None

    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = changes[-period:]
    gain = sum(max(value, 0.0) for value in recent) / period
    loss = sum(abs(min(value, 0.0)) for value in recent) / period
    if loss == 0:
        return 50.0 if gain == 0 else 100.0
    rs = gain / loss
    return 100.0 - (100.0 / (1.0 + rs))


def candle_trend(symbol):
    with MARKET_LOCK:
        candles = [
            c for c in MARKET_DATA[symbol]["candles"] if c["confirmed"]
        ][-3:]
    if len(candles) < 3:
        return "INSUFFICIENT_DATA"
    bullish = sum(c["close"] > c["open"] for c in candles)
    bearish = sum(c["close"] < c["open"] for c in candles)
    if bullish >= 2:
        return "BULLISH"
    if bearish >= 2:
        return "BEARISH"
    return "NEUTRAL"


def build_symbol_signal(symbol):
    if not price_is_fresh(symbol):
        return None

    momentum = calculate_momentum(symbol)
    trend = candle_trend(symbol)
    rsi = calculate_rsi(symbol)
    with MARKET_LOCK:
        price = MARKET_DATA[symbol]["price"]
        whale = copy.deepcopy(MARKET_DATA[symbol]["whale"])

    if momentum is None or rsi is None or trend in ("INSUFFICIENT_DATA", "NEUTRAL"):
        return None

    whale_bias = whale["net_bias"]
    direction = None
    if (
        CONFIG["allow_long"]
        and momentum >= CONFIG["momentum_threshold"]
        and trend == "BULLISH"
        and (not CONFIG["use_rsi_filter"] or rsi < CONFIG["rsi_long_ceiling"])
        and whale_bias > -0.65
    ):
        direction = "LONG"
    elif (
        CONFIG["allow_short"]
        and momentum <= -CONFIG["momentum_threshold"]
        and trend == "BEARISH"
        and (not CONFIG["use_rsi_filter"] or rsi > CONFIG["rsi_short_floor"])
        and whale_bias < 0.65
    ):
        direction = "SHORT"

    if direction is None:
        return None

    # Ranking score helps select one candidate from the entire watchlist.
    momentum_score = min(abs(momentum) / max(CONFIG["momentum_threshold"], 1e-9), 10.0)
    trend_score = 1.0
    whale_score = max(-1.0, min(1.0, whale_bias if direction == "LONG" else -whale_bias))
    rsi_quality = max(0.0, 1.0 - abs(rsi - (55.0 if direction == "LONG" else 45.0)) / 55.0)
    score = momentum_score + trend_score + whale_score + rsi_quality

    return {
        "symbol": symbol,
        "direction": direction,
        "price": price,
        "momentum": momentum,
        "rsi": rsi,
        "trend": trend,
        "whale_bias": whale_bias,
        "score": score,
        "timestamp": time.time(),
    }


# ============================================================
# 8. PORTFOLIO, POSITION SIZING AND PAPER EXECUTION
# ============================================================

def position_snapshot():
    with STATE_LOCK:
        return copy.deepcopy(OPEN_POSITION)


def has_open_position():
    with STATE_LOCK:
        return OPEN_POSITION is not None


def unrealized_pnl(position=None, current_price=None):
    if position is None:
        position = position_snapshot()
    if position is None:
        return 0.0
    if current_price is None:
        current_price = get_price(position["symbol"])
    if not valid_price(current_price):
        return 0.0
    if position["direction"] == "LONG":
        return (current_price - position["entry_price"]) * position["quantity"]
    return (position["entry_price"] - current_price) * position["quantity"]


def position_return_percent(position=None, current_price=None):
    if position is None:
        position = position_snapshot()
    if position is None:
        return 0.0
    if current_price is None:
        current_price = get_price(position["symbol"])
    if not valid_price(current_price):
        return 0.0
    if position["direction"] == "LONG":
        return ((current_price - position["entry_price"]) / position["entry_price"]) * 100.0
    return ((position["entry_price"] - current_price) / position["entry_price"]) * 100.0


def reserved_margin():
    position = position_snapshot()
    return 0.0 if position is None else float(position["margin_used"])


def update_account_equity():
    position = position_snapshot()
    pnl = unrealized_pnl(position) if position else 0.0
    with STATE_LOCK:
        ACCOUNT["equity"] = ACCOUNT["balance"] + pnl
        ACCOUNT["available_balance"] = max(0.0, ACCOUNT["balance"] - (
            0.0 if position is None else position["margin_used"]
        ))
        return ACCOUNT["equity"]


def account_is_healthy():
    update_account_equity()
    with STATE_LOCK:
        balance = ACCOUNT["balance"]
        equity = ACCOUNT["equity"]
    return (
        math.isfinite(balance)
        and math.isfinite(equity)
        and balance >= RISK_CONFIG["minimum_balance"]
        and equity > 0
    )


def calculate_position_size():
    with STATE_LOCK:
        balance = ACCOUNT["balance"]
    already_reserved = reserved_margin()
    available = max(0.0, balance - already_reserved)
    max_margin_for_exposure = (
        CONFIG["maximum_initial_exposure_usdt"] / CONFIG["leverage"]
    )
    margin = min(
        available * CONFIG["margin_fraction"],
        max_margin_for_exposure,
    )
    notional = min(
        margin * CONFIG["leverage"],
        CONFIG["maximum_initial_exposure_usdt"],
    )
    return margin, notional


def open_paper_position(signal):
    global OPEN_POSITION, LAST_SIGNAL

    if not PAPER_TRADING or REAL_TRADING or REAL_ORDERS_ENABLED:
        raise RuntimeError("Paper-only execution lock failed.")
    if not isinstance(signal, dict):
        return False

    symbol = signal.get("symbol")
    direction = signal.get("direction")
    entry = signal.get("price")

    if symbol not in WATCHLIST or direction not in ("LONG", "SHORT"):
        return False
    if direction == "LONG" and not CONFIG["allow_long"]:
        return False
    if direction == "SHORT" and not CONFIG["allow_short"]:
        return False
    if not valid_price(entry) or not price_is_fresh(symbol):
        return False

    with STATE_LOCK:
        if OPEN_POSITION is not None:
            return False
        balance = ACCOUNT["balance"]
        margin = min(
            balance * CONFIG["margin_fraction"],
            CONFIG["maximum_initial_exposure_usdt"] / CONFIG["leverage"],
        )
        notional = min(
            margin * CONFIG["leverage"],
            CONFIG["maximum_initial_exposure_usdt"],
        )
        if margin <= 0 or notional <= 0 or notional > CONFIG["maximum_initial_exposure_usdt"]:
            return False

        quantity = notional / float(entry)
        OPEN_POSITION = {
            "symbol": symbol,
            "direction": direction,
            "entry_price": float(entry),
            "quantity": quantity,
            "notional_value": notional,
            "margin_used": margin,
            "leverage": CONFIG["leverage"],
            "entry_time": time.time(),
            "entry_momentum": float(signal.get("momentum", 0.0)),
            "entry_rsi": float(signal.get("rsi", 50.0)),
            "entry_whale_bias": float(signal.get("whale_bias", 0.0)),
            "status": "OPEN",
        }
        LAST_SIGNAL = copy.deepcopy(signal)

    logger.info(
        "PAPER ENTRY | %s %s | entry=%.8f | notional=%.2f | margin=%.2f",
        direction, symbol, entry, notional, margin
    )
    save_state()
    return True


def close_paper_position(reason="MANUAL"):
    global OPEN_POSITION

    if not PAPER_TRADING or REAL_TRADING or REAL_ORDERS_ENABLED:
        raise RuntimeError("Paper-only execution lock failed.")

    with STATE_LOCK:
        if OPEN_POSITION is None:
            return False
        position = copy.deepcopy(OPEN_POSITION)

    symbol = position["symbol"]
    if not price_is_fresh(symbol):
        logger.warning("Exit deferred: %s price is stale.", symbol)
        return False
    exit_price = get_price(symbol)
    if not valid_price(exit_price):
        return False

    if position["direction"] == "LONG":
        pnl = (exit_price - position["entry_price"]) * position["quantity"]
    else:
        pnl = (position["entry_price"] - exit_price) * position["quantity"]
    if not math.isfinite(pnl):
        logger.error("Invalid simulated PnL; position retained.")
        return False

    with STATE_LOCK:
        # Recheck under lock in case another caller already closed it.
        if OPEN_POSITION is None:
            return False
        if OPEN_POSITION["entry_time"] != position["entry_time"]:
            return False

        ACCOUNT["balance"] += pnl
        ACCOUNT["realized_pnl"] += pnl
        ACCOUNT["total_trades"] += 1
        if pnl > 0:
            ACCOUNT["winning_trades"] += 1
        elif pnl < 0:
            ACCOUNT["losing_trades"] += 1

        TRADE_HISTORY.append({
            "symbol": symbol,
            "direction": position["direction"],
            "entry_price": position["entry_price"],
            "exit_price": exit_price,
            "quantity": position["quantity"],
            "notional_value": position["notional_value"],
            "pnl": pnl,
            "return_percent": position_return_percent(position, exit_price),
            "reason": reason,
            "entry_time": position["entry_time"],
            "exit_time": time.time(),
        })
        OPEN_POSITION = None
        ACCOUNT["equity"] = ACCOUNT["balance"]
        ACCOUNT["available_balance"] = ACCOUNT["balance"]

    logger.info(
        "PAPER EXIT | %s %s | exit=%.8f | pnl=%.4f USDT | reason=%s",
        position["direction"], symbol, exit_price, pnl, reason
    )
    save_state()
    return True


def manage_open_position():
    position = position_snapshot()
    if position is None:
        return False
    symbol = position["symbol"]
    if not price_is_fresh(symbol):
        return False

    update_account_equity()
    result = position_return_percent(position, get_price(symbol))

    if result <= -EXIT_CONFIG["stop_loss_percent"]:
        logger.warning("STOP LOSS reached: %.4f%%", result)
        return close_paper_position("STOP_LOSS")
    if result >= EXIT_CONFIG["take_profit_percent"]:
        logger.info("TAKE PROFIT reached: %.4f%%", result)
        return close_paper_position("TAKE_PROFIT")

    pnl = unrealized_pnl(position, get_price(symbol))
    margin = position["margin_used"]
    if margin > 0 and (pnl / margin) * 100.0 <= -RISK_CONFIG["emergency_loss_percent"]:
        logger.critical("Emergency loss protection triggered.")
        return close_paper_position("EMERGENCY_RISK_PROTECTION")
    return False


# ============================================================
# 9. INTELLIGENCE WORKERS
# ============================================================

def price_worker():
    logger.info("PRICE worker started.")
    while not BOT_STOP_EVENT.is_set():
        for symbol in WATCHLIST:
            if BOT_STOP_EVENT.is_set():
                break
            try:
                fetch_ticker(symbol)
            except Exception as error:
                global OKX_CONNECTED, LAST_CONNECTION_ERROR
                OKX_CONNECTED = False
                LAST_CONNECTION_ERROR = str(error)
                with MARKET_LOCK:
                    MARKET_DATA[symbol]["last_error"] = str(error)
                logger.warning("Ticker failed for %s: %s", symbol, error)
            WORKER_HEARTBEATS["price"] = time.time()
        BOT_STOP_EVENT.wait(CONFIG["price_poll_interval"])
    logger.info("PRICE worker stopped.")


def candle_worker():
    logger.info("CANDLE worker started.")
    while not BOT_STOP_EVENT.is_set():
        for symbol in WATCHLIST:
            if BOT_STOP_EVENT.is_set():
                break
            try:
                fetch_candles(symbol)
            except Exception as error:
                logger.warning("Candles failed for %s: %s", symbol, error)
            WORKER_HEARTBEATS["candles"] = time.time()
        BOT_STOP_EVENT.wait(CONFIG["candle_refresh_interval"])
    logger.info("CANDLE worker stopped.")


def whale_worker():
    logger.info("WHALE INTELLIGENCE worker started.")
    while not BOT_STOP_EVENT.is_set():
        for symbol in WATCHLIST:
            if BOT_STOP_EVENT.is_set():
                break
            try:
                fetch_whale_activity(symbol)
            except Exception as error:
                logger.warning("Public trade-flow scan failed for %s: %s", symbol, error)
            WORKER_HEARTBEATS["whale"] = time.time()
        BOT_STOP_EVENT.wait(CONFIG["whale_refresh_interval"])
    logger.info("WHALE INTELLIGENCE worker stopped.")


def market_scout_worker():
    logger.info("MARKET SCOUT worker started.")
    while not BOT_STOP_EVENT.is_set():
        candidates = []
        for symbol in WATCHLIST:
            if BOT_STOP_EVENT.is_set():
                break
            signal = build_symbol_signal(symbol)
            with MARKET_LOCK:
                MARKET_DATA[symbol]["signal"] = signal
                MARKET_DATA[symbol]["score"] = 0.0 if signal is None else signal["score"]
            if signal is not None:
                candidates.append(signal)

        candidates.sort(key=lambda item: item["score"], reverse=True)
        with MARKET_LOCK:
            for symbol in WATCHLIST:
                MARKET_DATA[symbol]["is_top_candidate"] = False
            if candidates:
                MARKET_DATA[candidates[0]["symbol"]]["is_top_candidate"] = True
        WORKER_HEARTBEATS["scout"] = time.time()
        BOT_STOP_EVENT.wait(CONFIG["scout_interval"])
    logger.info("MARKET SCOUT worker stopped.")


# ============================================================
# 10. ENGINE AND SUPERVISOR
# ============================================================

def engine_worker():
    global LAST_ENGINE_HEARTBEAT
    logger.info("ENGINE worker started.")

    while not BOT_STOP_EVENT.is_set():
        try:
            LAST_ENGINE_HEARTBEAT = time.time()
            WORKER_HEARTBEATS["engine"] = LAST_ENGINE_HEARTBEAT

            if has_open_position():
                # Existing position risk management always has priority.
                manage_open_position()
            elif account_is_healthy():
                with MARKET_LOCK:
                    candidates = [
                        copy.deepcopy(MARKET_DATA[symbol]["signal"])
                        for symbol in WATCHLIST
                        if MARKET_DATA[symbol].get("signal") is not None
                    ]
                candidates = [
                    item for item in candidates
                    if price_is_fresh(item["symbol"])
                ]
                if candidates:
                    best = max(candidates, key=lambda item: item["score"])
                    open_paper_position(best)

            if time.time() - LAST_SAVE_TIME >= CONFIG["save_interval"]:
                save_state()

        except Exception:
            logger.exception("ENGINE cycle error; continuing safely.")
        BOT_STOP_EVENT.wait(CONFIG["engine_interval"])

    logger.info("ENGINE worker stopped.")


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
        logger.exception("%s worker terminated unexpectedly.", name)


def trading_supervisor():
    global BOT_RUNNING, WORKER_THREADS, LAST_SUPERVISOR_HEARTBEAT

    threads = {}
    try:
        for name, target in WORKERS.items():
            thread = threading.Thread(
                target=_worker_entry,
                args=(name, target),
                name="NATZ1122_V611_" + name.upper(),
                daemon=True,
            )
            threads[name] = thread
            thread.start()

        WORKER_THREADS = threads
        logger.info("SUPERVISOR active; all V6.1.1 workers launched.")

        while not BOT_STOP_EVENT.wait(2.0):
            LAST_SUPERVISOR_HEARTBEAT = time.time()
            for name, target in WORKERS.items():
                thread = threads.get(name)
                if thread is None or not thread.is_alive():
                    if BOT_STOP_EVENT.is_set():
                        break
                    logger.error("%s worker exited; attempting restart.", name)
                    replacement = threading.Thread(
                        target=_worker_entry,
                        args=(name, target),
                        name="NATZ1122_V611_" + name.upper() + "_RESTART",
                        daemon=True,
                    )
                    threads[name] = replacement
                    replacement.start()
            WORKER_THREADS = threads

            # Fail-safe: if price data for an open position goes stale,
            # the engine will not calculate an exit from stale data.
            # It reports the stale condition and waits for fresh public data.
            position = position_snapshot()
            if position and not price_is_fresh(position["symbol"]):
                logger.warning("SUPERVISOR: open-position market price is stale.")

    finally:
        BOT_STOP_EVENT.set()
        for thread in threads.values():
            if thread.is_alive() and thread is not threading.current_thread():
                thread.join(timeout=5.0)
        save_state()
        BOT_RUNNING = False
        logger.info("SUPERVISOR stopped.")
        display_position_status()
        display_trade_statistics()


# ============================================================
# 11. STATUS AND REPORTS
# ============================================================

def market_data_status():
    print("\nV6.1.1 MARKET DATA STATUS")
    print("-" * 68)
    for symbol in WATCHLIST:
        with MARKET_LOCK:
            item = MARKET_DATA[symbol]
            price = item["price"]
            stamp = item["price_timestamp"]
            candles = len(item["candles"])
            whale = copy.deepcopy(item["whale"])
            error = item["last_error"]
        age = None if stamp is None else round(max(0.0, time.time() - stamp), 2)
        print(
            symbol,
            "| price:", price,
            "| age:", age,
            "| fresh:", price_is_fresh(symbol),
            "| candles:", candles,
            "| whale bias:", round(whale["net_bias"], 3),
            "| error:", error,
        )
    print("-" * 68)


def display_market_analysis():
    print("\n" + "=" * 68)
    print("NATZ1122 MARKET SCOUT V6.1.1")
    print("=" * 68)
    with MARKET_LOCK:
        rows = [
            (
                symbol,
                MARKET_DATA[symbol]["price"],
                MARKET_DATA[symbol]["score"],
                MARKET_DATA[symbol].get("signal"),
            )
            for symbol in WATCHLIST
        ]
    for symbol, price, score, signal in rows:
        if signal:
            detail = "{} {} | momentum={:.5f}% | RSI={:.2f}".format(
                signal["direction"], symbol, signal["momentum"], signal["rsi"]
            )
        else:
            detail = "No qualified entry"
        print(
            "{:<10} price={} score={:.3f} | {}".format(
                symbol, price, score, detail
            )
        )
    print("=" * 68)


def display_position_status():
    position = position_snapshot()
    update_account_equity()
    with STATE_LOCK:
        balance = ACCOUNT["balance"]
        equity = ACCOUNT["equity"]
        available = ACCOUNT["available_balance"]

    print("\n" + "=" * 62)
    print("NATZ1122 PAPER POSITION STATUS")
    print("=" * 62)
    if position is None:
        print("Position: NONE")
    else:
        symbol = position["symbol"]
        current = get_price(symbol)
        print("Symbol:", symbol)
        print("Direction:", position["direction"])
        print("Entry:", position["entry_price"])
        print("Current:", current)
        print("Quantity:", position["quantity"])
        print("Notional exposure:", round(position["notional_value"], 4), "USDT")
        print("Margin reserved:", round(position["margin_used"], 4), "USDT")
        print("Unrealized PNL:", round(unrealized_pnl(position, current), 4), "USDT")
        print("Underlying return:", round(position_return_percent(position, current), 4), "%")
        print("Price fresh:", price_is_fresh(symbol))
    print("Balance:", round(balance, 4), "USDT")
    print("Equity:", round(equity, 4), "USDT")
    print("Available balance:", round(available, 4), "USDT")
    print("=" * 62)


def display_trade_statistics():
    with STATE_LOCK:
        total = ACCOUNT["total_trades"]
        wins = ACCOUNT["winning_trades"]
        losses = ACCOUNT["losing_trades"]
        realized = ACCOUNT["realized_pnl"]
        balance = ACCOUNT["balance"]
    rate = (wins / total) * 100.0 if total else 0.0
    print("\n" + "=" * 62)
    print("NATZ1122 V6.1.1 TRADE STATISTICS")
    print("=" * 62)
    print("Total trades:", total)
    print("Wins:", wins)
    print("Losses:", losses)
    print("Win rate:", round(rate, 2), "%")
    print("Realized PNL:", round(realized, 4), "USDT")
    print("Balance:", round(balance, 4), "USDT")
    print("=" * 62)


def display_account_report():
    update_account_equity()
    with STATE_LOCK:
        account = copy.deepcopy(ACCOUNT)
        position = copy.deepcopy(OPEN_POSITION)
        history_count = len(TRADE_HISTORY)
    print("\n" + "=" * 68)
    print("NATZ1122 AI TRADING BOT V6.1.1 - ACCOUNT REPORT")
    print("=" * 68)
    print("Exchange:", CONFIG["exchange"])
    print("Watchlist:", ", ".join(WATCHLIST))
    print("Starting balance:", account["initial_balance"])
    print("Balance:", round(account["balance"], 4))
    print("Equity:", round(account["equity"], 4))
    print("Available balance:", round(account["available_balance"], 4))
    print("Realized PNL:", round(account["realized_pnl"], 4))
    print("Total trades:", account["total_trades"])
    print("Open position:", None if position is None else position["symbol"])
    print("Saved trade records:", history_count)
    print("Take profit:", EXIT_CONFIG["take_profit_percent"], "%")
    print("Stop loss:", EXIT_CONFIG["stop_loss_percent"], "%")
    print("Paper trading:", PAPER_TRADING)
    print("Real orders:", REAL_ORDERS_ENABLED)
    print("=" * 68)


def bot_status():
    print("\nNATZ1122 BOT STATUS")
    print("Version:", BOT_VERSION)
    print("Running:", BOT_RUNNING)
    print("Supervisor alive:", BOT_THREAD.is_alive() if BOT_THREAD else False)
    for name, thread in WORKER_THREADS.items():
        print("{} worker alive:".format(name), thread.is_alive())
    print("OKX public data connected:", OKX_CONNECTED)
    print("Paper trading:", PAPER_TRADING)
    print("Real trading:", REAL_TRADING)
    print("Real orders:", REAL_ORDERS_ENABLED)
    print("State file:", STATE_FILE)
    print("Worker heartbeats:", copy.deepcopy(WORKER_HEARTBEATS))
    display_position_status()


# ============================================================
# 12. LIFECYCLE AND MANUAL UTILITIES
# ============================================================

def start_bot():
    global BOT_RUNNING, BOT_THREAD

    with LIFECYCLE_LOCK:
        if BOT_RUNNING and BOT_THREAD is not None and BOT_THREAD.is_alive():
            print("V6.1.1 is already running.")
            return False

        validate_configuration()
        verify_paper_trading_safety()
        BOT_STOP_EVENT.clear()
        BOT_RUNNING = True
        BOT_THREAD = threading.Thread(
            target=trading_supervisor,
            name="NATZ1122_V611_SUPERVISOR",
            daemon=True,
        )
        BOT_THREAD.start()

    print("V6.1.1 START COMMAND SENT.")
    print("Verify all five workers using bot_status().")
    return True


def stop_bot():
    global BOT_RUNNING
    print("\nStopping NATZ1122 V6.1.1...")
    BOT_STOP_EVENT.set()
    thread = BOT_THREAD
    if thread is not None and thread.is_alive():
        thread.join(timeout=25.0)
    if thread is not None and thread.is_alive():
        print("Supervisor is still shutting down; check bot_status().")
    else:
        BOT_RUNNING = False
        print("V6.1.1 STOPPED.")
    if has_open_position():
        logger.warning("Open paper position preserved; stop does not auto-close it.")
        display_position_status()
    save_state()
    return not BOT_RUNNING


def stop_trading_bot():
    return stop_bot()


def monitor_market_once():
    print("Refreshing public market data once...")
    for symbol in WATCHLIST:
        try:
            fetch_ticker(symbol)
            fetch_candles(symbol)
            fetch_whale_activity(symbol)
        except Exception as error:
            logger.warning("Manual refresh failed for %s: %s", symbol, error)
    display_market_analysis()
    return True


def initialize_bot():
    validate_configuration()
    verify_paper_trading_safety()
    print("\nLoading V6.1.1 simulated account...")
    restored = load_state()
    if not restored:
        print("Fresh 50 USDT V6.1.1 account initialized.")
    update_account_equity()
    print("\nV6.1.1 ENGINE INITIALIZED - BOT NOT RUNNING")
    print("Storage directory:", BASE_DIR)
    print("State file:", STATE_FILE)
    display_account_report()
    return True


# Compatibility alias for application code that expects this name.
def initialize_engine():
    return initialize_bot()


# Importing this module initializes definitions only.
# The web application should call initialize_bot() once at startup.
# The bot does NOT start automatically on import.
