"""NATZ1122 AI Trading Bot V6.1.0 - standalone PWA backend.
Paper trading only. Public OKX market data only. No order endpoints or API keys.
"""
import base64
import hmac
import os
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBasic, HTTPBasicCredentials

import bot_engine as engine

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
APP_PASSWORD = os.environ.get("NATZ1122_APP_PASSWORD", "").strip()
if len(APP_PASSWORD) < 12:
    raise RuntimeError(
        "Set NATZ1122_APP_PASSWORD to a unique password of at least 12 characters. "
        "See README.md; the dashboard is password-protected."
    )

app = FastAPI(title="NATZ1122 AI Trading Bot", version="6.1.0", docs_url=None, redoc_url=None)
security = HTTPBasic()


def require_dashboard_password(credentials: HTTPBasicCredentials = Depends(security)):
    valid_user = hmac.compare_digest(credentials.username.encode(), b"natz")
    valid_password = hmac.compare_digest(credentials.password.encode(), APP_PASSWORD.encode())
    if not (valid_user and valid_password):
        raise HTTPException(
            status_code=401,
            detail="Dashboard password required",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


@app.on_event("startup")
def startup():
    engine.initialize_bot()


@app.on_event("shutdown")
def shutdown():
    if engine.BOT_RUNNING:
        engine.stop_bot()
    engine.save_state()


def _safe_float(value: Optional[float], digits=6):
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def snapshot():
    engine.update_account_equity()
    with engine.STATE_LOCK:
        account = dict(engine.ACCOUNT)
        position = dict(engine.OPEN_POSITION) if engine.OPEN_POSITION else None
        history = list(engine.TRADE_HISTORY[-30:])
    with engine.PRICE_LOCK:
        price = engine.LAST_PRICE
        price_ts = engine.LAST_PRICE_TIMESTAMP
        price_error = engine.LAST_PRICE_ERROR
        samples = len(engine.PRICE_HISTORY)
    with engine.CANDLE_LOCK:
        candle_count = len(engine.CANDLE_HISTORY)
        candle_ts = engine.LAST_CANDLE_TIMESTAMP
    momentum = None
    if engine.MOMENTUM_HISTORY:
        momentum = engine.MOMENTUM_HISTORY[-1].get("momentum")
    try:
        rsi = engine.calculate_rsi()
    except Exception:
        rsi = None
    try:
        trend = engine.analyze_market()
    except Exception:
        trend = "WAITING"
    now = time.time()
    price_age = None if price_ts is None else max(0.0, now - price_ts)
    pnl = engine.calculate_unrealized_pnl(price)
    position_return = engine.calculate_position_return(price)
    exposure = engine.calculate_position_exposure()
    total = account.get("total_trades", 0)
    wins = account.get("winning_trades", 0)
    return {
        "app": "NATZ1122 AI Trading Bot",
        "version": "6.1.0",
        "running": bool(engine.BOT_RUNNING),
        "exchange": "OKX",
        "symbol": "BTC-USDT",
        "timeframe": "1m",
        "price": _safe_float(price, 2),
        "price_age_seconds": _safe_float(price_age, 2),
        "price_fresh": bool(engine.is_price_fresh()),
        "price_error": price_error,
        "okx_connected": bool(engine.OKX_CONNECTED),
        "price_samples": samples,
        "candle_count": candle_count,
        "last_candle_timestamp": candle_ts,
        "momentum_percent": _safe_float(momentum, 6),
        "momentum_threshold_percent": 0.002,
        "rsi": _safe_float(rsi, 2),
        "trend": str(trend),
        "account": {
            "initial_balance": _safe_float(account.get("initial_balance"), 4),
            "balance": _safe_float(account.get("balance"), 4),
            "equity": _safe_float(account.get("equity"), 4),
            "realized_pnl": _safe_float(account.get("realized_pnl"), 4),
            "unrealized_pnl": _safe_float(pnl, 4),
            "total_trades": total,
            "winning_trades": wins,
            "losing_trades": account.get("losing_trades", 0),
            "win_rate": _safe_float((wins / total * 100) if total else 0, 2),
        },
        "position": None if position is None else {
            "direction": position.get("direction"),
            "entry_price": _safe_float(position.get("entry_price"), 2),
            "quantity": _safe_float(position.get("quantity"), 8),
            "notional_value": _safe_float(position.get("notional_value"), 4),
            "margin_used": _safe_float(position.get("margin_used"), 4),
            "leverage": position.get("leverage"),
            "entry_time": position.get("entry_time"),
            "unrealized_pnl": _safe_float(pnl, 4),
            "underlying_return_percent": _safe_float(position_return, 4),
        },
        "exposure_usdt": _safe_float(exposure, 4),
        "risk": {
            "maximum_initial_exposure_usdt": 100.0,
            "take_profit_underlying_percent": 10.0,
            "stop_loss_underlying_percent": 2.0,
            "leverage": 5.0,
            "long_enabled": True,
            "short_enabled": True,
        },
        "safety": {
            "paper_trading": True,
            "real_trading": False,
            "real_orders_enabled": False,
            "authenticated_exchange_api": False,
        },
        "workers": {
            "supervisor": bool(engine.BOT_THREAD and engine.BOT_THREAD.is_alive()),
            "price": bool(engine.PRICE_THREAD and engine.PRICE_THREAD.is_alive()),
            "candles": bool(engine.CANDLE_THREAD and engine.CANDLE_THREAD.is_alive()),
            "engine": bool(engine.ENGINE_THREAD and engine.ENGINE_THREAD.is_alive()),
        },
        "recent_trades": history,
        "server_time": now,
    }


@app.get("/api/status")
def api_status(_: bool = Depends(require_dashboard_password)):
    return JSONResponse(snapshot())


@app.post("/api/start")
def api_start(_: bool = Depends(require_dashboard_password)):
    if engine.BOT_RUNNING:
        return {"ok": True, "message": "Bot is already running.", "status": snapshot()}
    engine.start_bot()
    return {"ok": True, "message": "Paper-trading workers start requested.", "status": snapshot()}


@app.post("/api/stop")
def api_stop(_: bool = Depends(require_dashboard_password)):
    if engine.BOT_RUNNING:
        engine.stop_bot()
    return {"ok": True, "message": "Paper-trading bot stopped. Any open paper position is preserved.", "status": snapshot()}


@app.post("/api/close-position")
def api_close_position(_: bool = Depends(require_dashboard_password)):
    if not engine.has_open_position():
        raise HTTPException(status_code=409, detail="There is no open paper position.")
    if not engine.is_price_fresh():
        raise HTTPException(status_code=409, detail="Cannot close using stale market data. Wait for a fresh OKX price.")
    closed = engine.close_paper_position("MANUAL_FROM_DASHBOARD")
    if not closed:
        raise HTTPException(status_code=409, detail="Position could not be closed.")
    return {"ok": True, "message": "Paper position closed.", "status": snapshot()}


@app.get("/api/health")
def api_health():
    return {"ok": True, "version": "6.1.0", "paper_only": True}


@app.get("/manifest.webmanifest")
def manifest():
    return FileResponse(STATIC_DIR / "manifest.webmanifest", media_type="application/manifest+json")


@app.get("/service-worker.js")
def service_worker():
    return FileResponse(STATIC_DIR / "service-worker.js", media_type="application/javascript")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/{full_path:path}")
def frontend(full_path: str = ""):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(STATIC_DIR / "index.html")
