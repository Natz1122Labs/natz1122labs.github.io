# ============================================================
# NATZ1122 AI TRADING BOT - V6.1.0
# COMPLETE STANDALONE BUILD - FRESH PAPER SESSION
# PAPER TRADING ONLY - NO REAL ORDERS
#
# Fresh V6.1.0 session file:
#   /content/drive/MyDrive/NATZ1122_V6/v6_state_v610.json
# Legacy V6.0.8 state is archived separately and is never imported.
#
# IMPORTANT:
# - Uses OKX PUBLIC market-data endpoints only.
# - Contains no API keys, authenticated endpoints, or order endpoints.
# - Paper-trading results exclude fees, slippage, funding, and liquidation.
# - Colab runtimes can still disconnect or shut down; this code cannot
#   keep a cloud runtime alive after its host stops it.
# ============================================================

import os
import json
import time
import math
import copy
import shutil
import logging
import threading
import requests

from pathlib import Path
from datetime import datetime, timezone
from collections import deque

# ============================================================
# 1. IDENTITY AND CONFIGURATION
# ============================================================

BOT_NAME = "NATZ1122 AI TRADING BOT"
BOT_VERSION = "6.1.0"

CONFIG = {
    "exchange": "OKX",
    "symbol": "BTC-USDT",
    "timeframe": "1m",

    "allow_long": True,
    "allow_short": True,

    "starting_balance": 50.0,
    "leverage": 5.0,
    "margin_fraction": 1.0,
    "maximum_initial_exposure_usdt": 100.0,

    "momentum_threshold": 0.002,  # Percent, not decimal fraction
    "momentum_interval": 3.0,
    "momentum_tolerance": 1.25,

    "take_profit_percent": 10.0,  # Underlying BTC price movement
    "stop_loss_percent": 2.0,     # Underlying BTC price movement

    "use_rsi_filter": True,
    "rsi_period": 14,

    "price_poll_interval": 3.0,
    "candle_refresh_interval": 15.0,
    "engine_interval": 1.0,
    "candle_history": 200,

    "http_timeout": 10,
    "maximum_price_age": 12.0,
    "reconnect_interval": 3.0,
    "save_interval": 30.0,
    "heartbeat_warning_age": 15.0,

    "paper_trading": True,
    "real_trading": False,
    "allow_real_orders": False,

    "preserve_existing_state": True,  # Preserve V6.1.0 session across restarts only
    "backup_before_save": True,

    "log_level": "INFO",
}

PAPER_TRADING = True
REAL_TRADING = False
REAL_ORDERS_ENABLED = False

RISK_CONFIG = {
    "minimum_balance": 1.0,
    "maximum_position_fraction": 1.0,
    "maximum_leverage": 5.0,
    "emergency_loss_percent": 10.0,
    "reject_invalid_prices": True,
}

# Exit settings are mirrored for compatibility with V6.0.7 reports.
EXIT_CONFIG = {
    "take_profit_percent": CONFIG["take_profit_percent"],
    "stop_loss_percent": CONFIG["stop_loss_percent"],
}

assert PAPER_TRADING is True
assert REAL_TRADING is False
assert REAL_ORDERS_ENABLED is False
assert CONFIG["paper_trading"] is True
assert CONFIG["real_trading"] is False
assert CONFIG["allow_real_orders"] is False

print("=" * 68)
print(BOT_NAME)
print("VERSION:", BOT_VERSION)
print("MODE: PAPER TRADING ONLY - FRESH V6.1.0 SESSION")
print("EXCHANGE: OKX PUBLIC MARKET DATA")
print("=" * 68)

# ============================================================
# 2. STORAGE INITIALIZATION
# ============================================================

# Standalone application storage. Set NATZ1122_DATA_DIR to a persistent
# mounted volume on the deployment host before starting the application.
BASE_DIR = Path(os.environ.get("NATZ1122_DATA_DIR", str(Path(__file__).resolve().parent / "data")))
print("Standalone application storage:", BASE_DIR)

BASE_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR = BASE_DIR / "backups"
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

# V6.1.0 uses its own session file. The legacy V6.0.8 file is never loaded or overwritten.
LEGACY_V608_STATE_FILE = BASE_DIR / "v6_state.json"
STATE_FILE = BASE_DIR / "v6_state_v610.json"
BACKUP_FILE = BACKUP_DIR / "v6_state_v610_previous.json"
TEMP_STATE_FILE = BASE_DIR / "v6_state_v610.tmp"


def backup_legacy_v608_state():
    """Preserve the old V6.0.8 state as an archive; never import it."""
    if not LEGACY_V608_STATE_FILE.exists():
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive = BACKUP_DIR / ("v6_state_v608_legacy_" + stamp + ".json")
    # Avoid overwriting any prior archive, even in the unlikely event of a name collision.
    suffix = 1
    while archive.exists():
        archive = BACKUP_DIR / ("v6_state_v608_legacy_" + stamp + "_" + str(suffix) + ".json")
        suffix += 1
    shutil.copy2(LEGACY_V608_STATE_FILE, archive)
    logger.info("Previous V6.0.8 state archived separately: %s", archive)
    return archive

# ============================================================
# 3. LOGGING AND HTTP SESSION
# ============================================================

logging.basicConfig(
    level=getattr(logging, CONFIG["log_level"]),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger(BOT_NAME)

OKX_BASE_URL = "https://www.okx.com"
OKX_TICKER_ENDPOINT = OKX_BASE_URL + "/api/v5/market/ticker"
OKX_CANDLE_ENDPOINT = OKX_BASE_URL + "/api/v5/market/candles"

HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "NATZ1122-PaperTrading-App/6.1.0"})

# ============================================================
# 4. RUNTIME STATE AND LOCKS
# ============================================================

BOT_RUNNING = False
BOT_STOP_EVENT = threading.Event()

BOT_THREAD = None
PRICE_THREAD = None
CANDLE_THREAD = None
ENGINE_THREAD = None

STATE_LOCK = threading.RLock()
LIFECYCLE_LOCK = threading.RLock()
PRICE_LOCK = threading.RLock()
CANDLE_LOCK = threading.RLock()
PERSISTENCE_LOCK = threading.RLock()

PRICE_HISTORY = deque(maxlen=500)
MOMENTUM_HISTORY = deque(maxlen=100)
CANDLE_HISTORY = deque(maxlen=CONFIG["candle_history"])

LAST_PRICE = None
LAST_PRICE_TIMESTAMP = None
LAST_CANDLE_TIMESTAMP = None
LAST_SIGNAL = None

LAST_SAVE_TIME = 0.0
LAST_PRICE_ERROR = None
LAST_CANDLE_ERROR = None
LAST_CONNECTION_ERROR = None
LAST_ENGINE_HEARTBEAT = None
LAST_PRICE_HEARTBEAT = None
LAST_CANDLE_HEARTBEAT = None

OKX_CONNECTED = False
OPEN_POSITION = None
TRADE_HISTORY = []

ACCOUNT = {
    "initial_balance": CONFIG["starting_balance"],
    "balance": CONFIG["starting_balance"],
    "equity": CONFIG["starting_balance"],
    "total_trades": 0,
    "winning_trades": 0,
    "losing_trades": 0,
    "realized_pnl": 0.0,
}

# ============================================================
# 5. VALIDATION AND SAFETY
# ============================================================

def validate_configuration():
    required = {
        "symbol": "BTC-USDT",
        "timeframe": "1m",
        "allow_long": True,
        "allow_short": True,
        "starting_balance": 50.0,
        "leverage": 5.0,
        "margin_fraction": 1.0,
    "maximum_initial_exposure_usdt": 100.0,
        "momentum_threshold": 0.002,
        "momentum_interval": 3.0,
        "take_profit_percent": 10.0,
        "stop_loss_percent": 2.0,
    }
    for key, expected in required.items():
        if CONFIG.get(key) != expected:
            raise RuntimeError(
                "Configuration mismatch for {}: expected {}".format(
                    key, expected
                )
            )

    if not (PAPER_TRADING and not REAL_TRADING and not REAL_ORDERS_ENABLED):
        raise RuntimeError("Paper-trading safety lock failed.")

    logger.info("V6.1.0 configuration validation passed.")
    return True


def verify_paper_trading_safety():
    checks = {
        "Paper trading enabled": PAPER_TRADING is True,
        "Real trading disabled": REAL_TRADING is False,
        "Real orders disabled": REAL_ORDERS_ENABLED is False,
        "No authenticated trading API configured": True,
        "BTC-USDT selected": CONFIG["symbol"] == "BTC-USDT",
        "LONG enabled": CONFIG["allow_long"] is True,
        "SHORT enabled": CONFIG["allow_short"] is True,
        "5X simulated leverage": CONFIG["leverage"] == 5.0,
        "Maximum initial exposure capped at 100 USDT": CONFIG["maximum_initial_exposure_usdt"] == 100.0,
        "Momentum threshold 0.002%": CONFIG["momentum_threshold"] == 0.002,
        "Take profit 10%": EXIT_CONFIG["take_profit_percent"] == 10.0,
        "Stop loss 2%": EXIT_CONFIG["stop_loss_percent"] == 2.0,
    }

    print("\nV6.1.0 SAFETY VERIFICATION")
    print("-" * 58)
    for name, passed in checks.items():
        print(("PASS" if passed else "FAIL"), "-", name)

    if not all(checks.values()):
        raise RuntimeError("Safety verification failed.")

    print("-" * 58)
    print("ALL SAFETY CHECKS PASSED - PAPER TRADING ONLY.")
    print("FRESH SESSION: V6.0.8 positions and trade history are excluded.")
    return True


def validate_market_price(price):
    try:
        value = float(price)
        return math.isfinite(value) and value > 0
    except (TypeError, ValueError):
        return False


def is_price_fresh():
    with PRICE_LOCK:
        price = LAST_PRICE
        timestamp = LAST_PRICE_TIMESTAMP

    if price is None or timestamp is None:
        return False

    age = time.time() - timestamp
    return 0 <= age <= CONFIG["maximum_price_age"]


# ============================================================
# 6. STATE PERSISTENCE - V6.0.7 COMPATIBLE
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
    if not isinstance(data, dict):
        raise ValueError("Invalid state format.")

    account = data.get("account")
    if not isinstance(account, dict):
        raise ValueError("Missing account data.")

    required_account = (
        "initial_balance",
        "balance",
        "equity",
        "total_trades",
        "winning_trades",
        "losing_trades",
        "realized_pnl",
    )

    for key in required_account:
        if key not in account:
            raise ValueError("Missing account field: " + key)

        value = account[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("Invalid account value: " + key)
        if not math.isfinite(value):
            raise ValueError("Non-finite account value: " + key)

    if account["initial_balance"] <= 0 or account["balance"] < 0:
        raise ValueError("Invalid saved account balance.")

    for key in ("total_trades", "winning_trades", "losing_trades"):
        value = account[key]
        if not isinstance(value, int) or value < 0:
            raise ValueError("Invalid trade statistic: " + key)

    if account["winning_trades"] + account["losing_trades"] > account["total_trades"]:
        raise ValueError("Inconsistent saved trade statistics.")

    position = data.get("open_position")
    if position is not None:
        if not isinstance(position, dict):
            raise ValueError("Invalid saved position object.")

        required_position = (
            "direction", "entry_price", "quantity", "notional_value",
            "margin_used", "leverage", "entry_time", "status",
        )
        for key in required_position:
            if key not in position:
                raise ValueError("Missing saved position field: " + key)

        if position["direction"] not in ("LONG", "SHORT"):
            raise ValueError("Invalid saved position direction.")
        if position["status"] != "OPEN":
            raise ValueError("Invalid saved position status.")

        for key in (
            "entry_price", "quantity", "notional_value",
            "margin_used", "leverage", "entry_time",
        ):
            value = position[key]
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError("Invalid saved position value: " + key)
            if not math.isfinite(value) or value <= 0:
                raise ValueError("Invalid saved position value: " + key)

    history = data.get("trade_history", [])
    if not isinstance(history, list):
        raise ValueError("Invalid saved trade history.")

    return True


def save_state():
    global LAST_SAVE_TIME

    if not PAPER_TRADING or REAL_TRADING or REAL_ORDERS_ENABLED:
        raise RuntimeError("Persistence safety violation.")

    # One writer at a time: the engine, close handler and supervisor may
    # request a save close together.
    with PERSISTENCE_LOCK:
        data = build_state()

        try:
            with open(TEMP_STATE_FILE, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=4, allow_nan=False)
                file.flush()
                os.fsync(file.fileno())

            if STATE_FILE.exists() and CONFIG["backup_before_save"]:
                shutil.copy2(STATE_FILE, BACKUP_FILE)

            os.replace(str(TEMP_STATE_FILE), str(STATE_FILE))
            LAST_SAVE_TIME = time.time()
            logger.info("Account state saved.")
            return True

        except Exception:
            try:
                if TEMP_STATE_FILE.exists():
                    TEMP_STATE_FILE.unlink()
            except OSError:
                pass
            logger.exception("State saving failed; previous state file was retained.")
            return False


def load_state():
    global OPEN_POSITION, TRADE_HISTORY

    # A V6.1.0 session may be restored on a later restart. V6.0.8 state is
    # deliberately excluded so its old position/trades cannot enter this session.
    if not STATE_FILE.exists():
        backup_legacy_v608_state()
        with STATE_LOCK:
            ACCOUNT.update({
                "initial_balance": CONFIG["starting_balance"],
                "balance": CONFIG["starting_balance"],
                "equity": CONFIG["starting_balance"],
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "realized_pnl": 0.0,
            })
            OPEN_POSITION = None
            TRADE_HISTORY = []
        logger.info("Fresh V6.1.0 paper session created with 50.00 USDT.")
        save_state()
        return False

    with open(STATE_FILE, "r", encoding="utf-8") as file:
        data = json.load(file)

    validate_saved_state(data)

    # Reject accidental import of a state from a different bot version.
    if data.get("version") != BOT_VERSION:
        raise RuntimeError("V6.1.0 session file version mismatch; refusing unsafe restore.")

    with STATE_LOCK:
        saved_account = data["account"]
        for key in ACCOUNT:
            ACCOUNT[key] = saved_account[key]
        OPEN_POSITION = copy.deepcopy(data.get("open_position"))
        TRADE_HISTORY = copy.deepcopy(data.get("trade_history", []))

    logger.info("Previous V6.1.0 paper session restored.")
    logger.info("Restored V6.1.0 trade records: %d", len(TRADE_HISTORY))
    if OPEN_POSITION is not None:
        logger.warning("V6.1.0 OPEN paper position restored for same-session continuity.")
    return True

# ============================================================
# 7. OKX PUBLIC MARKET DATA
# ============================================================

def _okx_get(endpoint, params):
    response = HTTP.get(
        endpoint,
        params=params,
        timeout=CONFIG["http_timeout"],
    )
    response.raise_for_status()
    payload = response.json()

    if payload.get("code") != "0":
        raise RuntimeError(payload.get("msg", "OKX API returned an error."))

    return payload.get("data", [])


def get_live_price():
    global LAST_PRICE, LAST_PRICE_TIMESTAMP
    global OKX_CONNECTED, LAST_PRICE_ERROR, LAST_CONNECTION_ERROR
    global LAST_PRICE_HEARTBEAT

    try:
        data = _okx_get(
            OKX_TICKER_ENDPOINT,
            {"instId": CONFIG["symbol"]},
        )
        if not data:
            raise RuntimeError("OKX returned no ticker data.")

        price = float(data[0]["last"])
        if not validate_market_price(price):
            raise ValueError("Invalid BTC ticker price.")

        timestamp = time.time()
        with PRICE_LOCK:
            LAST_PRICE = price
            LAST_PRICE_TIMESTAMP = timestamp
            PRICE_HISTORY.append({"timestamp": timestamp, "price": price})

        OKX_CONNECTED = True
        LAST_PRICE_ERROR = None
        LAST_CONNECTION_ERROR = None
        LAST_PRICE_HEARTBEAT = timestamp
        return price

    except Exception as error:
        OKX_CONNECTED = False
        LAST_PRICE_ERROR = str(error)
        LAST_CONNECTION_ERROR = str(error)
        logger.warning("Ticker request failed: %s", error)
        return None


def get_market_candles(limit=None):
    global LAST_CANDLE_ERROR

    if limit is None:
        limit = CONFIG["candle_history"]
    limit = max(1, min(int(limit), 300))

    try:
        raw = _okx_get(
            OKX_CANDLE_ENDPOINT,
            {
                "instId": CONFIG["symbol"],
                "bar": CONFIG["timeframe"],
                "limit": str(limit),
            },
        )
        if not raw:
            raise RuntimeError("OKX returned no candle data.")

        candles = []
        for item in reversed(raw):
            try:
                candle = {
                    "timestamp": int(item[0]),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                    "confirmed": str(item[8]) == "1",
                }
                ohlc = (
                    candle["open"], candle["high"],
                    candle["low"], candle["close"],
                )
                if not all(validate_market_price(x) for x in ohlc):
                    continue
                if candle["high"] < candle["low"]:
                    continue
                if candle["high"] < max(candle["open"], candle["close"]):
                    continue
                if candle["low"] > min(candle["open"], candle["close"]):
                    continue
                candles.append(candle)
            except (IndexError, TypeError, ValueError):
                continue

        if not candles:
            raise RuntimeError("No valid candles received.")

        LAST_CANDLE_ERROR = None
        return candles

    except Exception as error:
        LAST_CANDLE_ERROR = str(error)
        logger.warning("Candle request failed: %s", error)
        return []


# ============================================================
# 8. INDEPENDENT DATA WORKERS
# ============================================================

def price_worker():
    global LAST_PRICE_HEARTBEAT

    logger.info("Price worker started.")
    while not BOT_STOP_EVENT.is_set():
        get_live_price()
        LAST_PRICE_HEARTBEAT = time.time()
        BOT_STOP_EVENT.wait(CONFIG["price_poll_interval"])
    logger.info("Price worker stopped.")


def candle_worker():
    global LAST_CANDLE_TIMESTAMP, LAST_CANDLE_HEARTBEAT

    logger.info("Candle worker started.")
    while not BOT_STOP_EVENT.is_set():
        candles = get_market_candles()

        if candles:
            with CANDLE_LOCK:
                CANDLE_HISTORY.clear()
                CANDLE_HISTORY.extend(candles)
                LAST_CANDLE_TIMESTAMP = candles[-1]["timestamp"]

        LAST_CANDLE_HEARTBEAT = time.time()
        BOT_STOP_EVENT.wait(CONFIG["candle_refresh_interval"])
    logger.info("Candle worker stopped.")


# ============================================================
# 9. MOMENTUM, RSI AND TREND
# ============================================================

def calculate_momentum():
    with PRICE_LOCK:
        samples = list(PRICE_HISTORY)

    if len(samples) < 2:
        return None

    current = samples[-1]
    target_time = current["timestamp"] - CONFIG["momentum_interval"]
    candidates = samples[:-1]
    previous = min(
        candidates,
        key=lambda item: abs(item["timestamp"] - target_time),
    )

    elapsed = current["timestamp"] - previous["timestamp"]
    if elapsed <= 0:
        return None

    if abs(elapsed - CONFIG["momentum_interval"]) > CONFIG["momentum_tolerance"]:
        return None

    old_price = previous["price"]
    new_price = current["price"]
    if old_price <= 0:
        return None

    momentum = ((new_price - old_price) / old_price) * 100.0
    MOMENTUM_HISTORY.append({
        "timestamp": current["timestamp"],
        "momentum": momentum,
    })
    return momentum


def calculate_rsi(period=None):
    if period is None:
        period = CONFIG["rsi_period"]

    with CANDLE_LOCK:
        closes = [
            candle["close"]
            for candle in CANDLE_HISTORY
            if candle["confirmed"]
        ]

    if len(closes) < period + 1:
        return None

    changes = [
        closes[i] - closes[i - 1]
        for i in range(1, len(closes))
    ]
    recent = changes[-period:]
    average_gain = sum(max(change, 0.0) for change in recent) / period
    average_loss = sum(abs(min(change, 0.0)) for change in recent) / period

    if average_loss == 0:
        return 50.0 if average_gain == 0 else 100.0

    rs = average_gain / average_loss
    return 100.0 - (100.0 / (1.0 + rs))


def analyze_candle_trend():
    with CANDLE_LOCK:
        candles = [
            candle for candle in CANDLE_HISTORY if candle["confirmed"]
        ]

    if len(candles) < 3:
        return "NEUTRAL"

    recent = candles[-3:]
    bullish = sum(c["close"] > c["open"] for c in recent)
    bearish = sum(c["close"] < c["open"] for c in recent)

    if bullish >= 2:
        return "BULLISH"
    if bearish >= 2:
        return "BEARISH"
    return "NEUTRAL"


def analyze_market():
    if not is_price_fresh():
        return "UNAVAILABLE"

    with CANDLE_LOCK:
        candle_count = len(CANDLE_HISTORY)

    if candle_count < 20:
        return "INSUFFICIENT_DATA"

    trend = analyze_candle_trend()
    rsi = calculate_rsi()

    if rsi is None:
        return "INSUFFICIENT_DATA"
    if trend == "BULLISH" and rsi < 70:
        return "BULLISH"
    if trend == "BEARISH" and rsi > 30:
        return "BEARISH"
    return "NEUTRAL"


def detect_entry_signal():
    global LAST_SIGNAL

    if not is_price_fresh():
        return None

    momentum = calculate_momentum()
    if momentum is None:
        return None

    trend = analyze_market()
    if trend in ("UNAVAILABLE", "INSUFFICIENT_DATA"):
        return None

    rsi = calculate_rsi()
    if rsi is None:
        return None

    with PRICE_LOCK:
        current_price = LAST_PRICE

    threshold = CONFIG["momentum_threshold"]

    if (
        CONFIG["allow_long"]
        and momentum >= threshold
        and trend == "BULLISH"
        and (not CONFIG["use_rsi_filter"] or rsi < 70)
    ):
        signal = {
            "direction": "LONG",
            "price": current_price,
            "momentum": momentum,
            "rsi": rsi,
            "trend": trend,
            "timestamp": time.time(),
        }
        LAST_SIGNAL = signal
        return signal

    if (
        CONFIG["allow_short"]
        and momentum <= -threshold
        and trend == "BEARISH"
        and (not CONFIG["use_rsi_filter"] or rsi > 30)
    ):
        signal = {
            "direction": "SHORT",
            "price": current_price,
            "momentum": momentum,
            "rsi": rsi,
            "trend": trend,
            "timestamp": time.time(),
        }
        LAST_SIGNAL = signal
        return signal

    return None


# ============================================================
# 10. ACCOUNT, POSITION AND RISK MANAGEMENT
# ============================================================

def has_open_position():
    with STATE_LOCK:
        return OPEN_POSITION is not None


def calculate_position_size():
    with STATE_LOCK:
        available_balance = ACCOUNT["balance"]

    if available_balance <= 0:
        return 0.0

    margin = min(
        available_balance * CONFIG["margin_fraction"],
        CONFIG["maximum_initial_exposure_usdt"] / CONFIG["leverage"],
    )
    return min(margin * CONFIG["leverage"], CONFIG["maximum_initial_exposure_usdt"])


def calculate_unrealized_pnl(current_price=None):
    with STATE_LOCK:
        position = copy.deepcopy(OPEN_POSITION)

    if position is None:
        return 0.0

    if current_price is None:
        with PRICE_LOCK:
            current_price = LAST_PRICE

    if not validate_market_price(current_price):
        return 0.0

    entry = position["entry_price"]
    quantity = position["quantity"]

    if position["direction"] == "LONG":
        return (current_price - entry) * quantity
    return (entry - current_price) * quantity


def calculate_position_return(current_price=None):
    with STATE_LOCK:
        position = copy.deepcopy(OPEN_POSITION)

    if position is None:
        return 0.0

    if current_price is None:
        with PRICE_LOCK:
            current_price = LAST_PRICE

    if not validate_market_price(current_price):
        return 0.0

    entry = position["entry_price"]
    if position["direction"] == "LONG":
        return ((current_price - entry) / entry) * 100.0
    return ((entry - current_price) / entry) * 100.0


def update_account_equity():
    unrealized = calculate_unrealized_pnl()
    with STATE_LOCK:
        ACCOUNT["equity"] = ACCOUNT["balance"] + unrealized
        return ACCOUNT["equity"]


def check_account_health():
    with STATE_LOCK:
        balance = ACCOUNT["balance"]
        equity = ACCOUNT["equity"]

    return (
        math.isfinite(balance)
        and math.isfinite(equity)
        and balance >= RISK_CONFIG["minimum_balance"]
        and equity > 0
    )


def calculate_position_exposure():
    with STATE_LOCK:
        if OPEN_POSITION is None:
            return 0.0
        return OPEN_POSITION["notional_value"]


def check_position_exposure():
    with STATE_LOCK:
        if OPEN_POSITION is None:
            return True
        exposure = OPEN_POSITION["notional_value"]
        balance = ACCOUNT["balance"]

    maximum = min(
        balance * RISK_CONFIG["maximum_leverage"] * RISK_CONFIG["maximum_position_fraction"],
        CONFIG["maximum_initial_exposure_usdt"],
    )
    return exposure <= maximum + 1e-9


def check_emergency_loss():
    with STATE_LOCK:
        position = copy.deepcopy(OPEN_POSITION)

    if position is None:
        return False

    pnl = calculate_unrealized_pnl()
    margin = position["margin_used"]
    if margin <= 0:
        return True

    loss_percent = (pnl / margin) * 100.0
    if loss_percent <= -RISK_CONFIG["emergency_loss_percent"]:
        logger.critical("EMERGENCY LOSS THRESHOLD: %.4f%%", loss_percent)
        return True
    return False


def validate_position_risk():
    if not PAPER_TRADING or REAL_TRADING or REAL_ORDERS_ENABLED:
        return False
    if not check_account_health() or not check_position_exposure():
        return False
    if has_open_position() and not is_price_fresh():
        return False
    return True


def open_paper_position(signal_data):
    global OPEN_POSITION

    if not PAPER_TRADING or REAL_TRADING or REAL_ORDERS_ENABLED:
        raise RuntimeError("Paper-trading safety lock violation.")
    if not signal_data or not is_price_fresh():
        return False

    direction = signal_data.get("direction")
    if direction not in ("LONG", "SHORT"):
        return False
    if direction == "LONG" and not CONFIG["allow_long"]:
        return False
    if direction == "SHORT" and not CONFIG["allow_short"]:
        return False

    entry_price = signal_data.get("price")
    if not validate_market_price(entry_price):
        return False

    # Recheck under the account lock to prevent duplicate entries.
    with STATE_LOCK:
        if OPEN_POSITION is not None:
            logger.warning("Duplicate entry rejected; a position is already open.")
            return False

        balance = ACCOUNT["balance"]
        margin = min(
            balance * CONFIG["margin_fraction"],
            CONFIG["maximum_initial_exposure_usdt"] / CONFIG["leverage"],
        )
        notional = min(margin * CONFIG["leverage"], CONFIG["maximum_initial_exposure_usdt"])

        if not math.isfinite(notional) or notional <= 0:
            return False

        quantity = notional / float(entry_price)
        OPEN_POSITION = {
            "direction": direction,
            "entry_price": float(entry_price),
            "quantity": quantity,
            "notional_value": notional,
            "margin_used": margin,
            "leverage": CONFIG["leverage"],
            "entry_time": time.time(),
            "entry_momentum": float(signal_data.get("momentum", 0.0)),
            "entry_rsi": float(signal_data.get("rsi", 50.0)),
            "status": "OPEN",
        }

    logger.info(
        "PAPER POSITION OPENED | %s | ENTRY %.2f | QTY %.8f | NOTIONAL %.2f",
        direction, entry_price, quantity, notional,
    )
    save_state()
    return True


def close_paper_position(reason="MANUAL"):
    global OPEN_POSITION

    if not PAPER_TRADING or REAL_TRADING or REAL_ORDERS_ENABLED:
        raise RuntimeError("Paper-trading safety lock violation.")
    if not is_price_fresh():
        logger.warning("Position closure deferred: market price is stale.")
        return False

    with PRICE_LOCK:
        exit_price = LAST_PRICE
    if not validate_market_price(exit_price):
        return False

    with STATE_LOCK:
        if OPEN_POSITION is None:
            return False

        position = copy.deepcopy(OPEN_POSITION)
        if position["direction"] == "LONG":
            pnl = (exit_price - position["entry_price"]) * position["quantity"]
        else:
            pnl = (position["entry_price"] - exit_price) * position["quantity"]

        if not math.isfinite(pnl):
            logger.error("Invalid PnL; position retained.")
            return False

        ACCOUNT["balance"] += pnl
        ACCOUNT["realized_pnl"] += pnl
        ACCOUNT["total_trades"] += 1

        if pnl > 0:
            ACCOUNT["winning_trades"] += 1
        elif pnl < 0:
            ACCOUNT["losing_trades"] += 1

        ACCOUNT["equity"] = ACCOUNT["balance"]

        TRADE_HISTORY.append({
            "direction": position["direction"],
            "entry_price": position["entry_price"],
            "exit_price": exit_price,
            "quantity": position["quantity"],
            "pnl": pnl,
            "reason": reason,
            "entry_time": position["entry_time"],
            "exit_time": time.time(),
        })
        OPEN_POSITION = None

    logger.info(
        "PAPER POSITION CLOSED | %s | PNL %.4f USDT | %s",
        position["direction"], pnl, reason,
    )
    save_state()
    return True


def check_stop_loss():
    if not has_open_position() or not is_price_fresh():
        return False
    result = calculate_position_return()
    if result <= -EXIT_CONFIG["stop_loss_percent"]:
        logger.warning("STOP LOSS TRIGGERED: %.4f%%", result)
        return close_paper_position("STOP_LOSS")
    return False


def check_take_profit():
    if not has_open_position() or not is_price_fresh():
        return False
    result = calculate_position_return()
    if result >= EXIT_CONFIG["take_profit_percent"]:
        logger.info("TAKE PROFIT TRIGGERED: %.4f%%", result)
        return close_paper_position("TAKE_PROFIT")
    return False


def check_emergency_exit():
    if has_open_position() and is_price_fresh() and check_emergency_loss():
        logger.critical("Emergency paper-position closure requested.")
        return close_paper_position("EMERGENCY_RISK_PROTECTION")
    return False


def manage_open_position():
    if not has_open_position():
        return False
    if not is_price_fresh():
        # Never calculate an exit from a stale price. Preserve the position.
        return False

    update_account_equity()

    if check_stop_loss():
        return True
    if check_take_profit():
        return True
    if check_emergency_exit():
        return True

    return False


# ============================================================
# 11. ENGINE WORKER AND SUPERVISOR
# ============================================================

def engine_worker():
    global LAST_ENGINE_HEARTBEAT

    logger.info("Trading engine worker started.")
    while not BOT_STOP_EVENT.is_set():
        try:
            LAST_ENGINE_HEARTBEAT = time.time()

            if is_price_fresh():
                update_account_equity()

                # Existing positions always take priority over new entries.
                if has_open_position():
                    manage_open_position()
                elif validate_position_risk():
                    signal = detect_entry_signal()
                    if signal:
                        logger.info(
                            "ENTRY SIGNAL | %s | PRICE %.2f | MOMENTUM %.6f%% | RSI %.2f",
                            signal["direction"],
                            signal["price"],
                            signal["momentum"],
                            signal["rsi"],
                        )
                        open_paper_position(signal)

            if time.time() - LAST_SAVE_TIME >= CONFIG["save_interval"]:
                save_state()

        except Exception:
            logger.exception("Engine cycle error; worker will continue.")

        BOT_STOP_EVENT.wait(CONFIG["engine_interval"])

    logger.info("Trading engine worker stopped.")


def _worker_entry(target, worker_name):
    try:
        target()
    except Exception:
        logger.exception("%s terminated unexpectedly.", worker_name)


def trading_supervisor():
    """Supervise independent workers and restart a worker if it exits."""
    global PRICE_THREAD, CANDLE_THREAD, ENGINE_THREAD, BOT_RUNNING

    workers = {
        "price": price_worker,
        "candles": candle_worker,
        "engine": engine_worker,
    }
    threads = {}

    try:
        for name, target in workers.items():
            thread = threading.Thread(
                target=_worker_entry,
                args=(target, name),
                name="NATZ1122_V610_" + name.upper(),
                daemon=True,
            )
            threads[name] = thread
            thread.start()

        PRICE_THREAD = threads["price"]
        CANDLE_THREAD = threads["candles"]
        ENGINE_THREAD = threads["engine"]

        logger.info("V6.1.0 supervisor active.")

        while not BOT_STOP_EVENT.wait(2.0):
            for name, target in workers.items():
                thread = threads.get(name)
                if thread is None or not thread.is_alive():
                    if BOT_STOP_EVENT.is_set():
                        break
                    logger.error("%s worker stopped; restarting it.", name)
                    replacement = threading.Thread(
                        target=_worker_entry,
                        args=(target, name),
                        name="NATZ1122_V610_" + name.upper(),
                        daemon=True,
                    )
                    threads[name] = replacement
                    replacement.start()

            PRICE_THREAD = threads.get("price")
            CANDLE_THREAD = threads.get("candles")
            ENGINE_THREAD = threads.get("engine")

    finally:
        BOT_STOP_EVENT.set()
        for thread in threads.values():
            if thread.is_alive() and thread is not threading.current_thread():
                thread.join(timeout=5.0)

        save_state()
        BOT_RUNNING = False
        logger.info("V6.1.0 supervisor stopped.")
        display_position_status()
        display_trade_statistics()


# ============================================================
# 12. STATUS AND REPORTS
# ============================================================

def market_data_status():
    with PRICE_LOCK:
        price = LAST_PRICE
        samples = len(PRICE_HISTORY)
        timestamp = LAST_PRICE_TIMESTAMP

    with CANDLE_LOCK:
        candle_count = len(CANDLE_HISTORY)
        candle_timestamp = LAST_CANDLE_TIMESTAMP

    age = None if timestamp is None else max(0.0, time.time() - timestamp)

    print("\nMARKET DATA STATUS")
    print("-" * 52)
    print("Exchange:", CONFIG["exchange"])
    print("Pair:", CONFIG["symbol"])
    print("Timeframe:", CONFIG["timeframe"])
    print("OKX connected:", OKX_CONNECTED)
    print("BTC price:", price)
    print("Price age (seconds):", None if age is None else round(age, 2))
    print("Fresh price:", is_price_fresh())
    print("Price samples:", samples)
    print("Candles:", candle_count)
    print("Latest candle timestamp:", candle_timestamp)
    print("Last price error:", LAST_PRICE_ERROR)
    print("Last candle error:", LAST_CANDLE_ERROR)
    print("-" * 52)


def display_market_analysis():
    momentum = MOMENTUM_HISTORY[-1]["momentum"] if MOMENTUM_HISTORY else None
    rsi = calculate_rsi()
    trend = analyze_market()

    with PRICE_LOCK:
        price = LAST_PRICE

    print("\n" + "=" * 58)
    print("NATZ1122 MARKET INTELLIGENCE V6.1.0")
    print("=" * 58)
    print("BTC-USDT:", price)
    print("Momentum:", "Waiting" if momentum is None else "{:.6f}%".format(momentum))
    print("RSI:", "Waiting" if rsi is None else round(rsi, 2))
    print("Trend:", trend)
    print("Threshold:", CONFIG["momentum_threshold"], "%")
    print("Directions: LONG + SHORT")
    print("Paper trading:", PAPER_TRADING)
    print("=" * 58)


def display_position_status():
    with STATE_LOCK:
        position = copy.deepcopy(OPEN_POSITION)
        balance = ACCOUNT["balance"]
        equity = ACCOUNT["equity"]

    with PRICE_LOCK:
        current = LAST_PRICE

    print("\n" + "=" * 58)
    print("NATZ1122 PAPER POSITION STATUS")
    print("=" * 58)

    if position is None:
        print("Position: NONE")
    else:
        print("Direction:", position["direction"])
        print("Entry:", position["entry_price"])
        print("Current:", current)
        print("Quantity:", position["quantity"])
        print("Unrealized PNL:", round(calculate_unrealized_pnl(), 4), "USDT")
        print("Underlying price return:", round(calculate_position_return(), 4), "%")
        print("Position status:", position["status"])
        if not is_price_fresh():
            print("WARNING: Price is stale; exit checks are waiting for fresh data.")

    print("Balance:", round(balance, 4))
    print("Equity:", round(equity, 4))
    print("=" * 58)


def display_risk_report():
    update_account_equity()
    print("\nRISK MANAGEMENT REPORT")
    print("-" * 52)
    print("Balance:", round(ACCOUNT["balance"], 4))
    print("Equity:", round(ACCOUNT["equity"], 4))
    print("Exposure:", round(calculate_position_exposure(), 4))
    print("Leverage:", CONFIG["leverage"], "X")
    print("Emergency loss threshold:", RISK_CONFIG["emergency_loss_percent"], "% of used margin")
    print("Risk validation:", validate_position_risk())
    print("Paper trading:", PAPER_TRADING)
    print("Real trading:", REAL_TRADING)
    print("-" * 52)


def display_trade_statistics():
    with STATE_LOCK:
        total = ACCOUNT["total_trades"]
        wins = ACCOUNT["winning_trades"]
        losses = ACCOUNT["losing_trades"]
        realized = ACCOUNT["realized_pnl"]
        balance = ACCOUNT["balance"]

    win_rate = (wins / total) * 100.0 if total else 0.0
    print("\n" + "=" * 58)
    print("NATZ1122 TRADE STATISTICS")
    print("=" * 58)
    print("Total trades:", total)
    print("Wins:", wins)
    print("Losses:", losses)
    print("Win rate:", round(win_rate, 2), "%")
    print("Realized PNL:", round(realized, 4), "USDT")
    print("Balance:", round(balance, 4), "USDT")
    print("=" * 58)


def display_account_report():
    update_account_equity()
    with STATE_LOCK:
        account = copy.deepcopy(ACCOUNT)
        open_position = copy.deepcopy(OPEN_POSITION)
        history_count = len(TRADE_HISTORY)

    print("\n" + "=" * 68)
    print("NATZ1122 AI TRADING BOT V6.1.0 - ACCOUNT REPORT")
    print("=" * 68)
    print("Exchange:", CONFIG["exchange"])
    print("Pair:", CONFIG["symbol"])
    print("Starting balance:", account["initial_balance"])
    print("Current balance:", round(account["balance"], 4))
    print("Current equity:", round(account["equity"], 4))
    print("Realized PNL:", round(account["realized_pnl"], 4))
    print("Total trades:", account["total_trades"])
    print("Wins:", account["winning_trades"])
    print("Losses:", account["losing_trades"])
    print("Open position:", open_position is not None)
    print("Saved trade records:", history_count)
    print("Paper trading:", PAPER_TRADING)
    print("Real orders:", REAL_ORDERS_ENABLED)
    print("=" * 68)


def bot_status():
    print("\nNATZ1122 BOT STATUS")
    print("Version:", BOT_VERSION)
    print("Running:", BOT_RUNNING)
    print("Supervisor alive:", BOT_THREAD.is_alive() if BOT_THREAD else False)
    print("Price worker alive:", PRICE_THREAD.is_alive() if PRICE_THREAD else False)
    print("Candle worker alive:", CANDLE_THREAD.is_alive() if CANDLE_THREAD else False)
    print("Engine worker alive:", ENGINE_THREAD.is_alive() if ENGINE_THREAD else False)
    print("OKX connected:", OKX_CONNECTED)
    print("Latest BTC price:", LAST_PRICE)
    print("Price fresh:", is_price_fresh())
    print("Last engine heartbeat:", LAST_ENGINE_HEARTBEAT)
    print("Last price heartbeat:", LAST_PRICE_HEARTBEAT)
    print("Last candle heartbeat:", LAST_CANDLE_HEARTBEAT)
    print("Paper trading:", PAPER_TRADING)
    print("Real trading:", REAL_TRADING)
    print("State file:", STATE_FILE)
    print("Open position:", has_open_position())
    display_position_status()


# ============================================================
# 13. MANUAL UTILITIES
# ============================================================

def monitor_market_once():
    price = get_live_price()
    if price is None:
        print("Price update failed.")
        return None

    candles = get_market_candles()
    if candles:
        with CANDLE_LOCK:
            CANDLE_HISTORY.clear()
            CANDLE_HISTORY.extend(candles)

    signal = detect_entry_signal()
    display_market_analysis()
    return signal


def start_bot():
    global BOT_RUNNING, BOT_THREAD

    with LIFECYCLE_LOCK:
        if BOT_RUNNING and BOT_THREAD is not None and BOT_THREAD.is_alive():
            print("Bot is already running.")
            return False

        verify_paper_trading_safety()
        BOT_STOP_EVENT.clear()
        BOT_RUNNING = True

        BOT_THREAD = threading.Thread(
            target=trading_supervisor,
            name="NATZ1122_V610_SUPERVISOR",
            daemon=True,
        )
        BOT_THREAD.start()

    print("V6.1.0 START COMMAND SENT.")
    print("Supervisor is starting independent price, candle and engine workers.")
    print("Use bot_status() to verify that all workers are alive.")
    return True


def stop_bot():
    global BOT_RUNNING

    print("\nStopping V6.1.0...")
    BOT_STOP_EVENT.set()

    thread = BOT_THREAD
    if thread is not None and thread.is_alive():
        thread.join(timeout=20.0)

    if thread is not None and thread.is_alive():
        print("Supervisor is still shutting down. Check bot_status() shortly.")
    else:
        BOT_RUNNING = False
        print("V6.1.0 STOPPED.")

    if has_open_position():
        logger.warning("OPEN PAPER POSITION PRESERVED; it was not auto-closed.")
        display_position_status()

    save_state()


def stop_trading_bot():
    return stop_bot()


# ============================================================
# 15. APPLICATION INITIALIZATION
# ============================================================

def initialize_bot():
    validate_configuration()
    verify_paper_trading_safety()
    print("\nLoading simulated account state...")
    restored = load_state()
    if not restored:
        print("Fresh simulated account initialized.")
    update_account_equity()
    print("\nV6.1.0 ENGINE INITIALIZED - BOT NOT RUNNING")
    display_account_report()
    return True

# The standalone web server calls initialize_bot() once during startup.
# Importing this module does not start workers or trading.
