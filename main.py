"""
Auto order bot FastAPI app.

Run with:
    uvicorn main:app --host 0.0.0.0 --port 8000
"""
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from sdks import ContractSDKFactory


CONFIG_PATH = Path(__file__).parent / "config.json"
LOG_PATH = Path(__file__).parent / "logs"
STATIC_DIR = Path(__file__).parent / "static"
CONTRACT_CACHE_TTL = timedelta(minutes=5)

LOG_PATH.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH / "auto_order.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("auto_order_bot")

scheduler = AsyncIOScheduler()
contract_symbol_cache: dict[str, dict[str, object]] = {}


def normalize_exchange(exchange: str | None, strict: bool = False) -> str:
    normalized_exchange = str(exchange or "binance").strip().lower()
    if normalized_exchange not in {"binance", "gateio", "bybit", "okx", "lighter"}:
        if strict:
            raise ValueError(f"Unsupported exchange: {exchange}")
        return "binance"
    return normalized_exchange


def default_symbol_for_exchange(exchange: str) -> str:
    if exchange == "gateio":
        return "BTC_USDT"
    if exchange == "okx":
        return "BTC-USDT-SWAP"
    if exchange == "lighter":
        return "BTC"
    return "BTCUSDT"


def default_quantity_for_exchange(exchange: str, quantity_mode: str) -> float:
    if quantity_mode == "margin":
        return 10.0
    if exchange == "gateio":
        return 1.0
    if exchange == "okx":
        return 0.01
    if exchange == "lighter":
        return 0.001
    return 0.001


def normalize_quantity_mode(value: str | None, default: str = "contract") -> str:
    normalized_mode = str(value or "").strip().lower()
    if normalized_mode in {"margin", "contract"}:
        return normalized_mode
    return default


def normalize_symbol(symbol: str | None, exchange: str) -> str:
    normalized_symbol = str(symbol or "").strip().upper().replace(" ", "")
    if exchange == "lighter":
        normalized_symbol = normalized_symbol.replace("_", "").replace("-", "").replace("/", "")
        for quote_suffix in ("USDT", "USDC", "USD"):
            if normalized_symbol.endswith(quote_suffix) and len(normalized_symbol) > len(quote_suffix):
                normalized_symbol = normalized_symbol[: -len(quote_suffix)]
                break
    return normalized_symbol or default_symbol_for_exchange(exchange)


def format_scheduled_at(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.replace(microsecond=0).isoformat(timespec="seconds")


def parse_scheduled_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).strip().replace(" ", "T"))
    except ValueError:
        return None


def parse_legacy_cron(value: str | None) -> datetime | None:
    cron = str(value or "").strip()
    if not cron or " " in cron:
        return None

    parts = cron.split(":")
    if len(parts) not in (2, 3):
        return None

    try:
        hour = int(parts[0])
        minute = int(parts[1])
        second = int(parts[2]) if len(parts) == 3 else 0
    except ValueError:
        return None

    now = datetime.now()
    scheduled_at = now.replace(hour=hour, minute=minute, second=second, microsecond=0)
    if scheduled_at < now:
        scheduled_at += timedelta(days=1)
    return scheduled_at


def normalize_task(task: dict) -> dict:
    normalized = dict(task)
    scheduled_at = parse_scheduled_at(normalized.get("scheduled_at"))
    if scheduled_at is None:
        scheduled_at = parse_legacy_cron(normalized.get("cron"))

    exchange = normalize_exchange(normalized.get("exchange", "binance"))
    if "quantity_mode" in normalized:
        quantity_mode = normalize_quantity_mode(normalized.get("quantity_mode"), default="margin")
    else:
        quantity_mode = "contract"
    default_quantity = default_quantity_for_exchange(exchange, quantity_mode)

    normalized["name"] = str(normalized.get("name", ""))
    normalized["exchange"] = exchange
    normalized["symbol"] = normalize_symbol(normalized.get("symbol"), exchange)
    normalized["quantity_mode"] = quantity_mode
    normalized["quantity"] = float(normalized.get("quantity", default_quantity))
    normalized["side"] = "short" if normalized.get("side") == "short" else "long"
    normalized["order_type"] = "limit" if normalized.get("order_type") == "limit" else "market"
    normalized["price"] = normalized.get("price")
    normalized["leverage"] = normalized.get("leverage")
    normalized["scheduled_at"] = format_scheduled_at(scheduled_at)
    normalized["enabled"] = normalized.get("enabled", True) is not False
    normalized.pop("cron", None)
    return normalized


def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as file:
            raw_config = json.load(file)
    else:
        raw_config = {}

    return {
        "tasks": [normalize_task(task) for task in raw_config.get("tasks", [])],
        "enabled": raw_config.get("enabled", True),
    }


def save_config(config: dict):
    normalized_config = {
        "tasks": [normalize_task(task) for task in config.get("tasks", [])],
        "enabled": config.get("enabled", True),
    }
    with open(CONFIG_PATH, "w", encoding="utf-8") as file:
        json.dump(normalized_config, file, ensure_ascii=False, indent=2)


def get_contract_symbols_payload(exchange: str, refresh: bool = False) -> dict:
    normalized_exchange = normalize_exchange(exchange, strict=True)
    now = datetime.now()
    cache_entry = contract_symbol_cache.get(normalized_exchange)

    if (
        not refresh
        and cache_entry
        and isinstance(cache_entry.get("updated_at"), datetime)
        and now - cache_entry["updated_at"] < CONTRACT_CACHE_TTL
    ):
        cached_symbols = list(cache_entry.get("symbols", []))
        updated_at = cache_entry["updated_at"]
        return {
            "exchange": normalized_exchange,
            "symbols": cached_symbols,
            "count": len(cached_symbols),
            "cached": True,
            "stale": False,
            "updated_at": updated_at.isoformat(timespec="seconds"),
        }

    try:
        sdk = ContractSDKFactory.get_sdk(normalized_exchange)
        symbols = sdk.list_contract_symbols()
        updated_at = datetime.now()
        contract_symbol_cache[normalized_exchange] = {
            "symbols": symbols,
            "updated_at": updated_at,
        }
        return {
            "exchange": normalized_exchange,
            "symbols": symbols,
            "count": len(symbols),
            "cached": False,
            "stale": False,
            "updated_at": updated_at.isoformat(timespec="seconds"),
        }
    except Exception as exc:
        logger.exception("Failed to load contract symbols for %s", normalized_exchange)
        if cache_entry:
            cached_symbols = list(cache_entry.get("symbols", []))
            updated_at = cache_entry.get("updated_at")
            return {
                "exchange": normalized_exchange,
                "symbols": cached_symbols,
                "count": len(cached_symbols),
                "cached": True,
                "stale": True,
                "updated_at": updated_at.isoformat(timespec="seconds") if isinstance(updated_at, datetime) else "",
                "message": str(exc),
            }
        raise HTTPException(status_code=502, detail=f"Failed to load {normalized_exchange} contracts: {exc}") from exc


def get_balance_payload(exchange: str) -> dict:
    normalized_exchange = normalize_exchange(exchange, strict=True)
    sdk = ContractSDKFactory.get_sdk(normalized_exchange)

    try:
        return sdk.get_balance()
    except NotImplementedError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Failed to load balance for %s", normalized_exchange)
        raise HTTPException(status_code=502, detail=f"Failed to load {normalized_exchange} balance: {exc}") from exc


def get_order_status_payload(exchange: str, order_id: str, symbol: str | None = None) -> dict:
    normalized_exchange = normalize_exchange(exchange, strict=True)
    sdk = ContractSDKFactory.get_sdk(normalized_exchange)

    try:
        return sdk.query_order_status(order_id, normalize_symbol(symbol, normalized_exchange) if symbol else None)
    except NotImplementedError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Failed to query order status for %s", normalized_exchange)
        raise HTTPException(status_code=502, detail=f"Failed to query {normalized_exchange} order status: {exc}") from exc


def run_scheduled_order(task: dict):
    try:
        exchange = normalize_exchange(task.get("exchange", "binance"), strict=True)
        symbol = normalize_symbol(task.get("symbol"), exchange)
        quantity = float(task.get("quantity", 0.001))
        quantity_mode = normalize_quantity_mode(task.get("quantity_mode"), default="contract")
        side = task.get("side", "long")
        order_type = task.get("order_type", "market")
        price = task.get("price")
        leverage = task.get("leverage")

        sdk = ContractSDKFactory.get_sdk(exchange)
        resolved_quantity_payload = sdk.resolve_order_quantity(
            symbol=symbol,
            quantity=quantity,
            quantity_mode=quantity_mode,
            leverage=int(leverage) if leverage else None,
            price=float(price) if price else None,
        )
        order_quantity = float(resolved_quantity_payload["quantity"])
        logger.info(
            "Resolved order quantity: exchange=%s symbol=%s mode=%s input=%s resolved=%s ref_price=%s leverage=%s",
            exchange,
            symbol,
            quantity_mode,
            quantity,
            order_quantity,
            resolved_quantity_payload.get("reference_price"),
            resolved_quantity_payload.get("leverage_used", leverage),
        )
        result = sdk.place_order(
            symbol=symbol,
            side=side,
            quantity=order_quantity,
            price=float(price) if price else None,
            order_type=order_type,
            leverage=int(leverage) if leverage else None,
        )
        logger.info("Scheduled task executed: %s -> %s", task, result)
        return result
    except Exception as exc:
        logger.error("Scheduled order failed: %s", exc)
        return {"success": False, "message": str(exc)}


def run_scheduled_order_by_index(task_index: int):
    config = load_config()
    tasks = config.get("tasks", [])

    if task_index >= len(tasks):
        logger.warning("Scheduled task index out of range: %s", task_index)
        return {"success": False, "message": "task not found"}

    task = tasks[task_index]
    if not task.get("enabled", True):
        logger.info("Skipped disabled task: %s", task.get("name", task_index))
        return {"success": False, "message": "task disabled"}

    result = run_scheduled_order(task)
    task["enabled"] = False
    save_config(config)
    logger.info("Task disabled after execution: %s", task.get("name", task_index))
    return result


def sync_scheduler():
    config = load_config()
    scheduler.remove_all_jobs()

    if not config.get("enabled", True):
        return

    now = datetime.now()
    for index, task in enumerate(config.get("tasks", [])):
        if not task.get("enabled", True):
            continue

        scheduled_at = parse_scheduled_at(task.get("scheduled_at"))
        if scheduled_at is None:
            logger.warning("Skipped task without valid schedule: %s", task)
            continue

        if scheduled_at < now - timedelta(seconds=30):
            logger.warning("Skipped expired task: %s @ %s", task.get("name", index + 1), task.get("scheduled_at"))
            continue

        run_date = scheduled_at if scheduled_at > now else now + timedelta(seconds=1)
        scheduler.add_job(
            run_scheduled_order_by_index,
            DateTrigger(run_date=run_date),
            args=[index],
            id=f"task_{index}",
            replace_existing=True,
            misfire_grace_time=60,
        )
        logger.info("Scheduled task added: %s @ %s", task.get("name", f"Task {index + 1}"), task.get("scheduled_at"))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    scheduler.start()
    sync_scheduler()
    yield
    scheduler.shutdown()


app = FastAPI(title="Auto Order Bot", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class TaskItem(BaseModel):
    name: str = ""
    exchange: str = "binance"
    symbol: str = "BTCUSDT"
    quantity_mode: str | None = None
    quantity: float = 10.0
    side: str = "long"
    order_type: str = "market"
    price: float | None = None
    leverage: int | None = None
    scheduled_at: str = ""
    enabled: bool = True


class ConfigUpdate(BaseModel):
    tasks: list[TaskItem] = Field(default_factory=list)
    enabled: bool = True


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = STATIC_DIR / "index.html"
    if html_path.exists():
        return FileResponse(html_path)
    return HTMLResponse("<h1>Please create static/index.html</h1>")


@app.get("/api/config")
async def get_config():
    return load_config()


@app.post("/api/config")
async def update_config(body: ConfigUpdate):
    config = load_config()
    config["tasks"] = [normalize_task(task.model_dump(exclude_none=True)) for task in body.tasks]
    config["enabled"] = body.enabled
    save_config(config)
    sync_scheduler()
    return {"ok": True}


@app.post("/api/execute")
async def execute_now(task: TaskItem):
    return run_scheduled_order(normalize_task(task.model_dump(exclude_none=True)))


@app.get("/api/exchanges")
async def list_exchanges():
    return ["binance", "gateio", "bybit", "okx", "lighter"]


@app.get("/api/contracts")
async def list_contract_symbols(
    exchange: str = Query(..., description="Exchange name, such as binance/gateio/bybit/okx/lighter"),
    refresh: bool = Query(False, description="Force refresh the server cache"),
):
    try:
        return get_contract_symbols_payload(exchange, refresh)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/balance")
async def get_balance(
    exchange: str = Query(..., description="Exchange name, such as lighter"),
):
    try:
        return get_balance_payload(exchange)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/order-status")
async def get_order_status(
    exchange: str = Query(..., description="Exchange name, such as lighter"),
    order_id: str = Query(..., description="Exchange order id or client order id"),
    symbol: str | None = Query(None, description="Optional symbol to narrow the market, such as BTC"),
):
    try:
        return get_order_status_payload(exchange, order_id, symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=2888)
