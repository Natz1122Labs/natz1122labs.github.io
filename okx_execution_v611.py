"""
NATZ1122 AI AUTO BOT V6.1.1 — PART 4
Authenticated OKX V5 execution adapter.

Supports LIVE USDT-margined linear SWAP market orders.
- Uses OKX REST authentication (HMAC-SHA256 + Base64).
- Calculates contract quantity from target USDT allocation and instrument metadata.
- Floors quantity to lotSz and enforces minSz.
- Enforces a configurable USDT allocation ceiling and one-open-position rule.
- Attaches exchange-side TP/SL algo instructions to the entry order.
- Uses clOrdId and checks order state after submission; never blindly resubmits
  after an ambiguous timeout.
- Defaults to DEMO mode. Live trading requires explicit environment switches.

IMPORTANT:
This module is live-capable, but a successful authenticated connection, accepted
order, filled order, and attached TP/SL must still be verified against the user's
own OKX account before declaring the integration operational.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


D = Decimal
OKX_BASE_URL = "https://www.okx.com"


class OKXExecutionError(RuntimeError):
    """Raised when OKX rejects a request or execution safety checks fail."""


class OKXAmbiguousExecutionError(OKXExecutionError):
    """Order submission outcome is uncertain; inspect order by clOrdId."""


def _dec(value: Any, field: str) -> Decimal:
    try:
        result = D(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise OKXExecutionError(f"Invalid numeric value for {field}: {value!r}")
    if not result.is_finite():
        raise OKXExecutionError(f"Non-finite numeric value for {field}")
    return result


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise OKXExecutionError("Exchange lot size must be positive")
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _fmt(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _safe_client_id() -> str:
    # OKX clOrdId: up to 32 alphanumeric characters.
    return "NATZ" + uuid.uuid4().hex[:28].upper()


@dataclass(frozen=True)
class OKXConfig:
    api_key: str
    api_secret: str
    passphrase: str
    demo_trading: bool = True
    live_trading_enabled: bool = False
    allocation_usdt: Decimal = D("10")
    max_allocation_usdt: Decimal = D("10")
    take_profit_pct: Decimal = D("2")
    stop_loss_pct: Decimal = D("0.75")
    max_open_positions: int = 1
    td_mode: str = "isolated"       # "isolated" or "cross"
    position_mode: str = "net"      # This implementation deliberately supports net mode only.
    tp_sl_trigger_type: str = "last"  # OKX accepts "last", "index", or "mark".
    request_timeout_seconds: int = 15

    @classmethod
    def from_env(cls) -> "OKXConfig":
        def flag(name: str, default: str = "false") -> bool:
            return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")

        return cls(
            api_key=os.getenv("OKX_API_KEY", ""),
            api_secret=os.getenv("OKX_API_SECRET", ""),
            passphrase=os.getenv("OKX_API_PASSPHRASE", ""),
            demo_trading=flag("OKX_DEMO_TRADING", "true"),
            live_trading_enabled=flag("OKX_LIVE_TRADING_ENABLED", "false"),
            allocation_usdt=_dec(os.getenv("OKX_ALLOCATION_USDT", "10"), "OKX_ALLOCATION_USDT"),
            max_allocation_usdt=_dec(os.getenv("OKX_MAX_ALLOCATION_USDT", "10"), "OKX_MAX_ALLOCATION_USDT"),
            take_profit_pct=_dec(os.getenv("OKX_TP_PCT", "2"), "OKX_TP_PCT"),
            stop_loss_pct=_dec(os.getenv("OKX_SL_PCT", "0.75"), "OKX_SL_PCT"),
            max_open_positions=int(os.getenv("OKX_MAX_OPEN_POSITIONS", "1")),
            td_mode=os.getenv("OKX_TD_MODE", "isolated").strip().lower(),
            position_mode=os.getenv("OKX_POSITION_MODE", "net").strip().lower(),
            tp_sl_trigger_type=os.getenv("OKX_TP_SL_TRIGGER_TYPE", "last").strip().lower(),
            request_timeout_seconds=int(os.getenv("OKX_TIMEOUT_SECONDS", "15")),
        )

    def validate(self) -> None:
        if not self.api_key or not self.api_secret or not self.passphrase:
            raise OKXExecutionError("Missing OKX credentials. Set OKX_API_KEY, OKX_API_SECRET, and OKX_API_PASSPHRASE.")
        if self.td_mode not in ("isolated", "cross"):
            raise OKXExecutionError("OKX_TD_MODE must be 'isolated' or 'cross'.")
        if self.position_mode != "net":
            raise OKXExecutionError("Only OKX net position mode is supported by this version. Set account position mode to net.")
        if self.allocation_usdt <= 0 or self.max_allocation_usdt <= 0:
            raise OKXExecutionError("USDT allocation values must be greater than zero.")
        if self.allocation_usdt > self.max_allocation_usdt:
            raise OKXExecutionError("Requested allocation exceeds OKX_MAX_ALLOCATION_USDT.")
        if self.take_profit_pct <= 0 or self.stop_loss_pct <= 0:
            raise OKXExecutionError("TP and SL percentages must be greater than zero.")
        if self.max_open_positions != 1:
            raise OKXExecutionError("This V6.1.1 Part 4 build enforces exactly one open position.")
        if self.tp_sl_trigger_type not in ("last", "index", "mark"):
            raise OKXExecutionError("TP/SL trigger type must be last, index, or mark.")
        if self.request_timeout_seconds < 3:
            raise OKXExecutionError("Request timeout must be at least 3 seconds.")
        if not self.demo_trading and not self.live_trading_enabled:
            raise OKXExecutionError("LIVE BLOCKED: set OKX_LIVE_TRADING_ENABLED=true only after demo verification.")
        if self.demo_trading and self.live_trading_enabled:
            raise OKXExecutionError("Conflicting mode flags: demo mode and live enable cannot both be selected.")


class OKXClient:
    def __init__(self, config: OKXConfig):
        config.validate()
        self.config = config
        self.base_url = OKX_BASE_URL
        self.last_response: Optional[Dict[str, Any]] = None

    def _timestamp(self) -> str:
        # OKX expects ISO-8601 UTC with milliseconds.
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int((time.time() % 1) * 1000):03d}Z"

    def _request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None,
                 authenticated: bool = True) -> Dict[str, Any]:
        method = method.upper()
        body_text = json.dumps(body, separators=(",", ":")) if body is not None else ""
        timestamp = self._timestamp()
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        if self.config.demo_trading:
            headers["x-simulated-trading"] = "1"

        if authenticated:
            prehash = timestamp + method + path + body_text
            digest = hmac.new(
                self.config.api_secret.encode("utf-8"),
                prehash.encode("utf-8"),
                hashlib.sha256,
            ).digest()
            signature = base64.b64encode(digest).decode("utf-8")
            headers.update({
                "OK-ACCESS-KEY": self.config.api_key,
                "OK-ACCESS-SIGN": signature,
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": self.config.passphrase,
            })

        request = Request(
            self.base_url + path,
            data=body_text.encode("utf-8") if body is not None else None,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=self.config.request_timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise OKXExecutionError(f"OKX HTTP {exc.code}: {raw[:1000]}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise OKXExecutionError(f"OKX network/request failure: {exc}") from exc

        try:
            result = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise OKXExecutionError(f"OKX returned invalid JSON: {raw[:500]}") from exc

        self.last_response = result
        if str(result.get("code", "")) != "0":
            raise OKXExecutionError(f"OKX API error {result.get('code')}: {result.get('msg', '')}")
        return result

    def _data(self, method: str, path: str, body: Optional[Dict[str, Any]] = None,
              authenticated: bool = True) -> list:
        result = self._request(method, path, body, authenticated)
        data = result.get("data")
        if not isinstance(data, list):
            raise OKXExecutionError(f"Unexpected OKX response structure for {path}")
        return data

    def get_instrument(self, inst_id: str) -> Dict[str, Any]:
        if not re.fullmatch(r"[A-Z0-9-]{3,40}", inst_id.upper()):
            raise OKXExecutionError("Invalid instrument ID.")
        rows = self._data("GET", f"/api/v5/public/instruments?instType=SWAP&instId={inst_id.upper()}", authenticated=False)
        if not rows:
            raise OKXExecutionError(f"OKX instrument not found: {inst_id}")
        instrument = rows[0]
        if instrument.get("state") != "live":
            raise OKXExecutionError(f"Instrument is not live/tradable: {instrument.get('state')}")
        if instrument.get("settleCcy") != "USDT" or instrument.get("ctType") != "linear":
            raise OKXExecutionError("Only USDT-settled linear SWAP contracts are supported.")
        return instrument

    def get_ticker_price(self, inst_id: str) -> Decimal:
        rows = self._data("GET", f"/api/v5/market/ticker?instId={inst_id.upper()}", authenticated=False)
        if not rows:
            raise OKXExecutionError("No ticker data returned by OKX.")
        price = _dec(rows[0].get("last"), "ticker.last")
        if price <= 0:
            raise OKXExecutionError("Ticker price is zero or invalid.")
        return price

    def get_positions(self) -> list:
        return self._data("GET", "/api/v5/account/positions?instType=SWAP")

    def get_open_positions(self) -> list:
        active = []
        for position in self.get_positions():
            try:
                size = _dec(position.get("pos", "0"), "position.pos")
            except OKXExecutionError:
                continue
            if size != 0:
                active.append(position)
        return active

    def get_order(self, inst_id: str, cl_ord_id: str) -> Optional[Dict[str, Any]]:
        rows = self._data(
            "GET",
            f"/api/v5/trade/order?instId={inst_id.upper()}&clOrdId={cl_ord_id}",
        )
        return rows[0] if rows else None

    def _contracts_for_allocation(self, instrument: Dict[str, Any], price: Decimal,
                                   allocation_usdt: Decimal) -> Decimal:
        ct_val = _dec(instrument.get("ctVal"), "ctVal")
        lot_sz = _dec(instrument.get("lotSz"), "lotSz")
        min_sz = _dec(instrument.get("minSz"), "minSz")
        ct_val_ccy = instrument.get("ctValCcy", "")

        if ct_val <= 0 or lot_sz <= 0 or min_sz <= 0:
            raise OKXExecutionError("Invalid ctVal, lotSz, or minSz returned by OKX.")

        # For linear USDT swaps, contract value is normally expressed in base currency.
        if ct_val_ccy == instrument.get("baseCcy"):
            raw_contracts = allocation_usdt / (price * ct_val)
        elif ct_val_ccy == "USDT":
            raw_contracts = allocation_usdt / ct_val
        else:
            raise OKXExecutionError(
                f"Unsupported contract value currency {ct_val_ccy!r}; refusing unsafe size conversion."
            )

        contracts = _floor_to_step(raw_contracts, lot_sz)
        if contracts < min_sz:
            min_notional = min_sz * ct_val * price if ct_val_ccy == instrument.get("baseCcy") else min_sz * ct_val
            raise OKXExecutionError(
                f"Allocation is below exchange minimum. Minimum size is {min_sz} contracts "
                f"(approx. {min_notional} USDT notional); increase authorized allocation or skip this symbol."
            )

        # Ensure rounding never causes notional to exceed the authorized allocation.
        notional = contracts * ct_val * price if ct_val_ccy == instrument.get("baseCcy") else contracts * ct_val
        if notional > allocation_usdt:
            raise OKXExecutionError("Rounded contract quantity exceeds authorized USDT allocation.")
        return contracts

    def _build_attached_tp_sl(self, side: str, entry_price: Decimal) -> list:
        if side == "buy":
            tp = entry_price * (D("1") + self.config.take_profit_pct / D("100"))
            sl = entry_price * (D("1") - self.config.stop_loss_pct / D("100"))
        else:
            tp = entry_price * (D("1") - self.config.take_profit_pct / D("100"))
            sl = entry_price * (D("1") + self.config.stop_loss_pct / D("100"))
        if tp <= 0 or sl <= 0:
            raise OKXExecutionError("Calculated TP/SL price is invalid.")
        return [{
            "tpTriggerPx": _fmt(tp),
            "tpOrdPx": "-1",
            "tpTriggerPxType": self.config.tp_sl_trigger_type,
            "slTriggerPx": _fmt(sl),
            "slOrdPx": "-1",
            "slTriggerPxType": self.config.tp_sl_trigger_type,
        }]

    def execute_market_entry(self, inst_id: str, direction: str,
                             allocation_usdt: Optional[Any] = None) -> Dict[str, Any]:
        """
        Execute one market entry.

        direction: "long" or "short"
        allocation_usdt: optional per-trade allocation override; must not exceed
                         config.max_allocation_usdt.
        """
        symbol = inst_id.upper().strip()
        side_map = {"long": "buy", "short": "sell"}
        if direction.lower().strip() not in side_map:
            raise OKXExecutionError("direction must be 'long' or 'short'.")
        side = side_map[direction.lower().strip()]

        allocation = self.config.allocation_usdt if allocation_usdt is None else _dec(allocation_usdt, "allocation_usdt")
        if allocation <= 0:
            raise OKXExecutionError("Trade allocation must be greater than zero.")
        if allocation > self.config.max_allocation_usdt:
            raise OKXExecutionError("Trade allocation exceeds the configured authorized maximum.")

        # Global one-position rule: do not open if ANY SWAP position is already active.
        open_positions = self.get_open_positions()
        if len(open_positions) >= self.config.max_open_positions:
            raise OKXExecutionError(
                f"Position limit reached ({len(open_positions)} active SWAP position(s)); no new order sent."
            )

        instrument = self.get_instrument(symbol)
        reference_price = self.get_ticker_price(symbol)
        contracts = self._contracts_for_allocation(instrument, reference_price, allocation)
        client_order_id = _safe_client_id()

        order = {
            "instId": symbol,
            "tdMode": self.config.td_mode,
            "side": side,
            "ordType": "market",
            "sz": _fmt(contracts),
            "clOrdId": client_order_id,
            "attachAlgoOrds": self._build_attached_tp_sl(side, reference_price),
        }

        # Do not automatically retry a place-order request after a timeout.
        try:
            submitted = self._data("POST", "/api/v5/trade/order", order)
        except OKXExecutionError as exc:
            # If the request failed ambiguously, query by the same client ID.
            # If no order is found, still do not blindly submit a second order.
            try:
                found = self.get_order(symbol, client_order_id)
            except Exception:
                found = None
            if found:
                return {
                    "status": "found_after_submission_error",
                    "instId": symbol,
                    "clOrdId": client_order_id,
                    "order": found,
                    "message": "Order found by client ID after submission error; inspect state before proceeding.",
                }
            raise OKXAmbiguousExecutionError(
                f"Order submission outcome is uncertain. No automatic retry was made. "
                f"Check OKX order history using clOrdId={client_order_id}. Original error: {exc}"
            ) from exc

        if not submitted:
            raise OKXExecutionError("OKX returned no order acknowledgement.")
        acknowledgement = submitted[0]
        if str(acknowledgement.get("sCode", "")) != "0":
            raise OKXExecutionError(
                f"OKX rejected order: sCode={acknowledgement.get('sCode')} "
                f"sMsg={acknowledgement.get('sMsg', '')}"
            )

        # Confirm order record by client ID. An accepted acknowledgement is not proof of fill.
        time.sleep(0.4)
        verified = self.get_order(symbol, client_order_id)
        if not verified:
            return {
                "status": "submitted_unverified",
                "instId": symbol,
                "clOrdId": client_order_id,
                "acknowledgement": acknowledgement,
                "message": "OKX acknowledged submission, but order record was not yet retrievable. Do not resubmit.",
            }

        state = verified.get("state", "unknown")
        return {
            "status": "order_verified",
            "instId": symbol,
            "clOrdId": client_order_id,
            "side": side,
            "contracts": _fmt(contracts),
            "reference_price": _fmt(reference_price),
            "requested_allocation_usdt": _fmt(allocation),
            "order_state": state,
            "filled_size": verified.get("accFillSz", "0"),
            "average_fill_price": verified.get("avgPx", ""),
            "tp_sl_attached_in_request": True,
            "order": verified,
            "message": (
                "Order record verified. Confirm filled size and TP/SL algo orders in OKX "
                "before treating the position as protected."
            ),
        }


def build_executor() -> OKXClient:
    """Create a configured client from environment variables."""
    return OKXClient(OKXConfig.from_env())


def execute_signal(signal: Dict[str, Any], executor: Optional[OKXClient] = None) -> Dict[str, Any]:
    """
    Adapter entry point for the V6.1.1 ENGINE.

    Expected signal example:
        {"instId": "BTC-USDT-SWAP", "direction": "long", "allocation_usdt": "10"}

    Call this only after the ENGINE and SUPERVISOR approve the signal.
    """
    if not isinstance(signal, dict):
        raise OKXExecutionError("Signal must be a dictionary.")
    inst_id = signal.get("instId")
    direction = signal.get("direction")
    if not inst_id or not direction:
        raise OKXExecutionError("Signal requires instId and direction.")
    client = executor or build_executor()
    return client.execute_market_entry(
        inst_id=str(inst_id),
        direction=str(direction),
        allocation_usdt=signal.get("allocation_usdt"),
    )


if __name__ == "__main__":
    # Connection test only. This does NOT place an order.
    client = build_executor()
    account = client._request("GET", "/api/v5/account/config")
    config_row = (account.get("data") or [{}])[0]
    print(json.dumps({
        "connection": "authenticated_okx_api_ok",
        "demo_trading": client.config.demo_trading,
        "live_trading_enabled": client.config.live_trading_enabled,
        "position_mode": config_row.get("posMode"),
        "account_level": config_row.get("acctLv"),
        "message": "Connection test only; no order was placed."
    }, indent=2))
