
# ============================================================
# NATZ1122 AI AUTO BOT V6.1.1
# COMPLETE RECONSTRUCTION
# PART 1 OF 8 — CORE FOUNDATION
# ============================================================

import os
import json
import time
import math
import hmac
import base64
import hashlib
import logging
import threading
import secrets
import uuid
import copy
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests

# ============================================================
# 1. BOT IDENTITY
# ============================================================

BOT_NAME = "NATZ1122 AI AUTO BOT"
BOT_VERSION = "6.1.1"
EXCHANGE_NAME = "OKX"

# ============================================================
# 2. ENVIRONMENT CONFIGURATION
# ============================================================

def env_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def env_float(name, default):
    try:
        value = float(os.getenv(name, default))
        if not math.isfinite(value):
            raise ValueError("Non-finite value")
        return value
    except (TypeError, ValueError):
        return float(default)


def env_int(name, default):
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return int(default)


# ============================================================
# 3. TRADING CONFIGURATION
# ============================================================

CONFIG = {
    "exchange": "OKX",

    # DEMO or LIVE
    # Live integration is part of this application.
    "trading_mode": os.getenv(
        "OKX_TRADING_MODE", "demo"
    ).strip().lower(),

    "instrument_type": "SWAP",
    "margin_mode": "isolated",
    "position_mode": "net",

    # Variable trading allocation
    "trading_allocation_usdt": env_float(
        "TRADING_ALLOCATION_USDT", 50.0
    ),

    "minimum_allocation_usdt": 5.0,

    "maximum_allocation_usdt": env_float(
        "MAX_TRADING_ALLOCATION_USDT", 50.0
    ),

    # Adjustable leverage
    "leverage": env_float("BOT_LEVERAGE", 2.0),
    "maximum_leverage": env_float(
        "MAX_LEVERAGE", 2.0
    ),

    # Exposure controls
    "maximum_exposure_usdt": env_float(
        "MAX_EXPOSURE_USDT", 50.0
    ),

    "maximum_open_positions": 1,

    "risk_per_trade_percent": 1.0,

    "maximum_daily_loss_percent": 5.0,

    "emergency_loss_percent": 10.0,

    # Trading exits
    "take_profit_percent": 2.0,
    "stop_loss_percent": 0.75,

    # Automatic multi-coin selection
    "watchlist": [
        "BTC-USDT-SWAP",
        "ETH-USDT-SWAP",
        "SOL-USDT-SWAP",
        "XRP-USDT-SWAP",
        "DOGE-USDT-SWAP",
        "ADA-USDT-SWAP",
    ],

    "automatic_coin_selection": True,

    "allow_long": True,
    "allow_short": True,

    # Strategy
    "timeframe": "1m",
    "rsi_period": 14,
    "momentum_threshold": 0.002,

    # Worker intervals
    "price_interval": 2.0,
    "candle_interval": 15.0,
    "whale_interval": 10.0,
    "scout_interval": 3.0,
    "engine_interval": 1.0,

    # Reliability
    "http_timeout": 10,
    "maximum_http_retries": 3,
    "maximum_price_age": 15.0,
    "maximum_signal_age": 8.0,

    # Persistence
    "save_interval": 20.0,

    # Cloud storage
    "data_directory": os.getenv(
        "NATZ1122_DATA_DIR",
        str(Path(__file__).resolve().parent / "data")
    ),

    # Emergency controls
    "emergency_stop": False,
}

# ============================================================
# 4. CONFIGURATION VALIDATION
# ============================================================

def validate_configuration():

    if CONFIG["trading_mode"] not in ("demo", "live"):
        raise ValueError(
            "Trading mode must be demo or live."
        )

    if CONFIG["instrument_type"] != "SWAP":
        raise ValueError(
            "This V6.1.1 build is configured for OKX SWAP."
        )

    if CONFIG["margin_mode"] not in ("isolated", "cross"):
        raise ValueError("Invalid margin mode.")

    allocation = CONFIG["trading_allocation_usdt"]

    if not (
        CONFIG["minimum_allocation_usdt"]
        <= allocation
        <= CONFIG["maximum_allocation_usdt"]
    ):
        raise ValueError(
            "Trading allocation is outside permitted limits."
        )

    if not (
        1 <= CONFIG["leverage"]
        <= CONFIG["maximum_leverage"]
    ):
        raise ValueError("Invalid leverage configuration.")

    if CONFIG["maximum_exposure_usdt"] <= 0:
        raise ValueError("Invalid maximum exposure.")

    if CONFIG["maximum_open_positions"] < 1:
        raise ValueError("Invalid position limit.")

    if CONFIG["take_profit_percent"] != 2.0:
        raise ValueError("TP must remain 2%.")

    if CONFIG["stop_loss_percent"] != 0.75:
        raise ValueError("SL must remain 0.75%.")

    if len(set(CONFIG["watchlist"])) != len(CONFIG["watchlist"]):
        raise ValueError("Duplicate instruments in watchlist.")

    return True


# ============================================================
# 5. EXIT CONFIGURATION
# ============================================================

EXIT_CONFIG = {
    "take_profit_percent": 2.0,
    "stop_loss_percent": 0.75,
}

# ============================================================
# 6. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    ),
)

logger = logging.getLogger(BOT_NAME)

# ============================================================
# 7. GLOBAL RUNTIME STATE
# ============================================================

BOT_RUNNING = False
BOT_INITIALIZED = False

BOT_STOP_EVENT = threading.Event()

BOT_LOCK = threading.RLock()
ACCOUNT_LOCK = threading.RLock()
POSITION_LOCK = threading.RLock()
ENGINE_LOCK = threading.RLock()
SUPERVISOR_LOCK = threading.RLock()

WORKER_THREADS = {}

WORKER_HEARTBEATS = {}

MARKET_DATA = {}
CANDLE_DATA = {}
WHALE_DATA = {}
SCOUT_SIGNALS = {}

SUPERVISOR_STATUS = {}
ENGINE_STATUS = {}

OPEN_POSITIONS = {}
TRADE_HISTORY = []

ACCOUNT_STATE = {
    "starting_balance": 50.0,
    "available_balance": 0.0,
    "equity": 0.0,
    "realized_pnl": 0.0,
    "unrealized_pnl": 0.0,
    "total_trades": 0,
    "winning_trades": 0,
    "losing_trades": 0,
}

# ============================================================
# 8. STORAGE INITIALIZATION
# ============================================================

BASE_DIR = Path(CONFIG["data_directory"])

BASE_DIR.mkdir(
    parents=True,
    exist_ok=True
)

BACKUP_DIR = BASE_DIR / "backups"

BACKUP_DIR.mkdir(
    parents=True,
    exist_ok=True
)

STATE_FILE = BASE_DIR / "natz1122_v611_state.json"

LOG_FILE = BASE_DIR / "natz1122_v611.log"

# ============================================================
# 9. UNIQUE ORDER IDENTIFIERS
# ============================================================

def create_client_order_id():

    return (
        "NATZ"
        + uuid.uuid4().hex[:28].upper()
    )

# ============================================================
# END OF PART 1 OF 8
# NEXT: OKX AUTHENTICATION AND MARKET CONNECTIVITY
# ============================================================

# ============================================================
# NATZ1122 AI AUTO BOT V6.1.1
# PART 2 OF 8 — OKX API CLIENT AND AUTHENTICATION
# ============================================================

from urllib.parse import urlencode


OKX_BASE_URL = "https://www.okx.com"


class OKXAPIError(Exception):
    """Raised when OKX returns an API or connection error."""
    pass


class OKXClient:
    """
    Authenticated OKX V5 API client.

    Credentials must be stored in environment variables:
      OKX_API_KEY
      OKX_API_SECRET
      OKX_API_PASSPHRASE

    Demo trading uses the x-simulated-trading header.
    """

    def __init__(self):
        self.api_key = os.getenv("OKX_API_KEY", "").strip()
        self.api_secret = os.getenv("OKX_API_SECRET", "").strip()
        self.passphrase = os.getenv("OKX_API_PASSPHRASE", "").strip()

        self.trading_mode = str(
            CONFIG.get("trading_mode", "demo")
        ).lower()

        self.timeout = int(CONFIG.get("http_timeout", 15))
        self.max_retries = int(
            CONFIG.get("maximum_http_retries", 3)
        )

        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "NATZ1122-AI-AUTO-BOT-V6.1.1"
        })

        self._validate_credentials()

    def _validate_credentials(self):
        if self.trading_mode not in ("demo", "live"):
            raise ValueError("Invalid OKX trading mode.")

        if not self.api_key or not self.api_secret or not self.passphrase:
            raise ValueError(
                "Missing OKX API credentials. Configure "
                "OKX_API_KEY, OKX_API_SECRET and "
                "OKX_API_PASSPHRASE in your hosting environment."
            )

    @staticmethod
    def _timestamp():
        return (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

    def _signature(self, timestamp, method, request_path, body=""):
        message = (
            timestamp
            + method.upper()
            + request_path
            + body
        )

        digest = hmac.new(
            self.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256
        ).digest()

        return base64.b64encode(digest).decode("utf-8")

    def _headers(self, method, request_path, body=""):
        timestamp = self._timestamp()

        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": self._signature(
                timestamp,
                method,
                request_path,
                body
            ),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase
        }

        if self.trading_mode == "demo":
            headers["x-simulated-trading"] = "1"

        return headers

    def request(self, method, endpoint, params=None,
                payload=None, authenticated=False):

        method = method.upper()
        params = params or {}

        query = urlencode(params, doseq=True)
        request_path = endpoint

        if method == "GET" and query:
            request_path += "?" + query

        body = ""

        if method != "GET" and payload is not None:
            body = json.dumps(
                payload,
                separators=(",", ":"),
                ensure_ascii=False
            )

        headers = {}

        if authenticated:
            headers = self._headers(
                method,
                request_path,
                body
            )

        url = OKX_BASE_URL + request_path

        attempts = self.max_retries if method == "GET" else 1
        attempts = max(1, attempts)

        last_error = None

        for attempt in range(attempts):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    data=body if body else None,
                    timeout=self.timeout
                )

                response.raise_for_status()
                result = response.json()

                if not isinstance(result, dict):
                    raise OKXAPIError(
                        "Unexpected response format from OKX."
                    )

                if str(result.get("code", "0")) != "0":
                    raise OKXAPIError(
                        "OKX API error "
                        + str(result.get("code"))
                        + ": "
                        + str(result.get("msg", "Unknown error"))
                    )

                return result

            except requests.RequestException as exc:
                last_error = exc

                if attempt + 1 < attempts:
                    time.sleep(min(2 ** attempt, 8))

            except ValueError as exc:
                last_error = exc
                break

        raise OKXAPIError(
            "OKX request failed: " + str(last_error)
        )

    # --------------------------------------------------------
    # PUBLIC MARKET DATA
    # --------------------------------------------------------

    def get_ticker(self, instrument_id):
        result = self.request(
            "GET",
            "/api/v5/market/ticker",
            params={"instId": instrument_id}
        )

        rows = result.get("data", [])

        if not rows:
            raise OKXAPIError(
                "No ticker data for " + instrument_id
            )

        return rows[0]

    def get_candles(self, instrument_id,
                    bar=None, limit=100):

        bar = bar or CONFIG.get("timeframe", "1m")

        result = self.request(
            "GET",
            "/api/v5/market/candles",
            params={
                "instId": instrument_id,
                "bar": bar,
                "limit": str(limit)
            }
        )

        return result.get("data", [])

    def get_instrument(self, instrument_id):
        result = self.request(
            "GET",
            "/api/v5/public/instruments",
            params={
                "instType": "SWAP",
                "instId": instrument_id
            }
        )

        rows = result.get("data", [])

        if not rows:
            raise OKXAPIError(
                "Instrument not found: " + instrument_id
            )

        return rows[0]

    # --------------------------------------------------------
    # AUTHENTICATED ACCOUNT DATA
    # --------------------------------------------------------

    def get_account_balance(self, currency=None):
        params = {}

        if currency:
            params["ccy"] = currency

        result = self.request(
            "GET",
            "/api/v5/account/balance",
            params=params,
            authenticated=True
        )

        rows = result.get("data", [])
        return rows[0] if rows else {}

    def get_account_configuration(self):
        result = self.request(
            "GET",
            "/api/v5/account/config",
            authenticated=True
        )

        rows = result.get("data", [])
        return rows[0] if rows else {}

    def get_positions(self, instrument_id=None):
        params = {}

        if instrument_id:
            params["instId"] = instrument_id

        result = self.request(
            "GET",
            "/api/v5/account/positions",
            params=params,
            authenticated=True
        )

        return result.get("data", [])

    # --------------------------------------------------------
    # ACCOUNT AND LEVERAGE CONFIGURATION
    # --------------------------------------------------------

    def set_leverage(self, instrument_id, leverage=None):
        leverage = leverage or CONFIG.get("leverage", 2)

        payload = {
            "instId": instrument_id,
            "lever": str(leverage),
            "mgnMode": CONFIG.get("margin_mode", "isolated")
        }

        return self.request(
            "POST",
            "/api/v5/account/set-leverage",
            payload=payload,
            authenticated=True
        )

    # --------------------------------------------------------
    # ORDER SUBMISSION
    # --------------------------------------------------------

    def place_order(self, instrument_id, side, quantity,
                    order_type="market", price=None,
                    reduce_only=False, attached_algos=None,
                    client_order_id=None):

        side = str(side).lower()

        if side not in ("buy", "sell"):
            raise ValueError("Order side must be buy or sell.")

        quantity = Decimal(str(quantity))

        if quantity <= 0:
            raise ValueError("Order quantity must be positive.")

        payload = {
            "instId": instrument_id,
            "tdMode": CONFIG.get("margin_mode", "isolated"),
            "side": side,
            "ordType": order_type,
            "sz": format(quantity, "f"),
            "clOrdId": (
                client_order_id or create_client_order_id()
            )
        }

        if order_type == "limit":
            if price is None:
                raise ValueError(
                    "Limit orders require a price."
                )

            payload["px"] = str(price)

        if reduce_only:
            payload["reduceOnly"] = "true"

        if attached_algos:
            payload["attachAlgoOrds"] = attached_algos

        return self.request(
            "POST",
            "/api/v5/trade/order",
            payload=payload,
            authenticated=True
        )

    def cancel_order(self, instrument_id, order_id=None,
                     client_order_id=None):

        if not order_id and not client_order_id:
            raise ValueError(
                "An order ID or client order ID is required."
            )

        payload = {"instId": instrument_id}

        if order_id:
            payload["ordId"] = str(order_id)

        if client_order_id:
            payload["clOrdId"] = str(client_order_id)

        return self.request(
            "POST",
            "/api/v5/trade/cancel-order",
            payload=payload,
            authenticated=True
        )

    # --------------------------------------------------------
    # PROTECTIVE ALGO ORDERS
    # --------------------------------------------------------

    def place_algo_order(self, payload):
        return self.request(
            "POST",
            "/api/v5/trade/order-algo",
            payload=payload,
            authenticated=True
        )

    def cancel_algo_orders(self, orders):
        if not orders:
            return None

        return self.request(
            "POST",
            "/api/v5/trade/cancel-algos",
            payload=orders,
            authenticated=True
        )

    def get_pending_algo_orders(self, instrument_id=None):
        params = {
            "ordType": "conditional"
        }

        if instrument_id:
            params["instId"] = instrument_id

        result = self.request(
            "GET",
            "/api/v5/trade/orders-algo-pending",
            params=params,
            authenticated=True
        )

        return result.get("data", [])

    # --------------------------------------------------------
    # ORDER STATUS
    # --------------------------------------------------------

    def get_order(self, instrument_id, order_id=None,
                  client_order_id=None):

        params = {"instId": instrument_id}

        if order_id:
            params["ordId"] = str(order_id)
        elif client_order_id:
            params["clOrdId"] = str(client_order_id)
        else:
            raise ValueError(
                "An order ID or client order ID is required."
            )

        result = self.request(
            "GET",
            "/api/v5/trade/order",
            params=params,
            authenticated=True
        )

        rows = result.get("data", [])
        return rows[0] if rows else {}

    def get_open_orders(self, instrument_id=None):
        params = {}

        if instrument_id:
            params["instId"] = instrument_id

        result = self.request(
            "GET",
            "/api/v5/trade/orders-pending",
            params=params,
            authenticated=True
        )

        return result.get("data", [])


# ============================================================
# END OF PART 2 OF 8
# ============================================================
# ============================================================
# 9. INTELLIGENCE WORKERS - PART 3
# Four independent workers with concurrent per-symbol collection,
# per-source freshness checks, health reporting and error isolation.
# ============================================================


def _record_worker_result(worker, symbol=None, error=None):
    """Record an attempt without allowing one symbol failure to stop a worker."""
    now = time.time()

    with MARKET_LOCK:
        status = WORKER_STATUS[worker]
        status["last_cycle"] = now

        if error is None:
            status["last_success"] = now
            status["last_error"] = None
            status["consecutive_errors"] = 0
            status["successful_symbols"] += 1
        else:
            message = str(error)[:500]
            status["last_error"] = message
            status["consecutive_errors"] += 1
            status["failed_symbols"] += 1

        if symbol in MARKET_DATA:
            source = MARKET_DATA[symbol]["source_status"][worker]
            source["last_attempt"] = now

            if error is None:
                source["last_success"] = now
                source["last_error"] = None
                source["consecutive_errors"] = 0
            else:
                source["last_error"] = str(error)[:500]
                source["consecutive_errors"] += 1
                MARKET_DATA[symbol]["last_error"] = str(error)[:500]


def _source_is_fresh(symbol, source):
    """Return True only when the requested source has a recent valid update."""

    with MARKET_LOCK:
        item = MARKET_DATA.get(symbol)

        if item is None:
            return False

        if source == "price":
            stamp = item.get("price_timestamp")
            max_age = CONFIG["maximum_price_age"]

        elif source == "candles":
            stamp = item.get("candle_timestamp")
            max_age = CONFIG["maximum_candle_age"]

        elif source == "whale":
            stamp = item.get("whale", {}).get("timestamp")
            max_age = CONFIG["maximum_whale_age"]

        else:
            return False

    if stamp is None:
        return False

    age = time.time() - stamp

    return 0 <= age <= max_age


def worker_health_snapshot():
    """Return a thread-safe snapshot for status screens and diagnostics."""

    with MARKET_LOCK:
        return copy.deepcopy(WORKER_STATUS)


def _run_symbol_batch(worker, fetch_function):
    """Fetch all watchlist symbols concurrently; isolate each request failure."""

    with ThreadPoolExecutor(
        max_workers=len(WATCHLIST),
        thread_name_prefix="NATZ1122_" + worker.upper(),
    ) as pool:

        futures = {
            pool.submit(fetch_function, symbol): symbol
            for symbol in WATCHLIST
        }

        for future in as_completed(futures):
            symbol = futures[future]

            if BOT_STOP_EVENT.is_set():
                continue

            try:
                future.result()

                _record_worker_result(
                    worker,
                    symbol=symbol,
                )

            except Exception as error:
                _record_worker_result(
                    worker,
                    symbol=symbol,
                    error=error,
                )

                logger.warning(
                    "%s update failed for %s: %s",
                    worker.upper(),
                    symbol,
                    error,
                )

    WORKER_HEARTBEATS[worker] = time.time()

    WORKER_STATUS[worker]["last_cycle"] = (
        WORKER_HEARTBEATS[worker]
    )


# ============================================================
# PRICE WORKER
# ============================================================

def price_worker():

    logger.info(
        "PRICE worker started; six symbols update concurrently."
    )

    while not BOT_STOP_EVENT.is_set():

        _run_symbol_batch(
            "price",
            fetch_ticker,
        )

        BOT_STOP_EVENT.wait(
            CONFIG["price_poll_interval"]
        )

    logger.info("PRICE worker stopped.")


# ============================================================
# CANDLE INTELLIGENCE WORKER
# ============================================================

def candle_worker():

    logger.info(
        "CANDLE worker started; six symbols update concurrently."
    )

    while not BOT_STOP_EVENT.is_set():

        _run_symbol_batch(
            "candles",
            fetch_candles,
        )

        BOT_STOP_EVENT.wait(
            CONFIG["candle_refresh_interval"]
        )

    logger.info("CANDLE worker stopped.")


# ============================================================
# WHALE INTELLIGENCE WORKER
# ============================================================

def whale_worker():

    logger.info(
        "WHALE INTELLIGENCE worker started; six symbols update concurrently."
    )

    while not BOT_STOP_EVENT.is_set():

        _run_symbol_batch(
            "whale",
            fetch_whale_activity,
        )

        BOT_STOP_EVENT.wait(
            CONFIG["whale_refresh_interval"]
        )

    logger.info("WHALE INTELLIGENCE worker stopped.")


# ============================================================
# MARKET SCOUT WORKER
# ============================================================

def market_scout_worker():

    logger.info("MARKET SCOUT worker started.")

    while not BOT_STOP_EVENT.is_set():

        candidates = []

        for symbol in WATCHLIST:

            if BOT_STOP_EVENT.is_set():
                break

            try:
                # A candidate is considered only when all three
                # independent intelligence feeds are fresh.

                feeds_fresh = all(
                    _source_is_fresh(symbol, source)
                    for source in (
                        "price",
                        "candles",
                        "whale",
                    )
                )

                signal = (
                    build_symbol_signal(symbol)
                    if feeds_fresh
                    else None
                )

                with MARKET_LOCK:

                    MARKET_DATA[symbol]["signal"] = signal

                    MARKET_DATA[symbol]["score"] = (
                        0.0
                        if signal is None
                        else signal["score"]
                    )

                _record_worker_result(
                    "scout",
                    symbol=symbol,
                )

                if signal is not None:
                    candidates.append(signal)

            except Exception as error:

                with MARKET_LOCK:

                    MARKET_DATA[symbol]["signal"] = None
                    MARKET_DATA[symbol]["score"] = 0.0

                _record_worker_result(
                    "scout",
                    symbol=symbol,
                    error=error,
                )

                logger.exception(
                    "MARKET SCOUT failed for %s.",
                    symbol,
                )

        # Rank all valid candidates.

        candidates.sort(
            key=lambda item: item["score"],
            reverse=True,
        )

        # Mark the strongest candidate.

        with MARKET_LOCK:

            for symbol in WATCHLIST:
                MARKET_DATA[symbol]["is_top_candidate"] = False

            if candidates:

                MARKET_DATA[
                    candidates[0]["symbol"]
                ]["is_top_candidate"] = True

        WORKER_HEARTBEATS["scout"] = time.time()

        WORKER_STATUS["scout"]["last_cycle"] = (
            WORKER_HEARTBEATS["scout"]
        )

        BOT_STOP_EVENT.wait(
            CONFIG["scout_interval"]
        )

    logger.info("MARKET SCOUT worker stopped.")


# ============================================================
# END OF PART 3
# PART 4: ENGINE, SUPERVISOR AND EXECUTION
# ============================================================
