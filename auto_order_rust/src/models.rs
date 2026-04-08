use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Timelike};
use serde::{Deserialize, Serialize};

pub const SUPPORTED_EXCHANGES: [&str; 4] = ["binance", "gateio", "bybit", "okx"];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskItem {
    #[serde(default)]
    pub name: String,
    #[serde(default = "default_exchange")]
    pub exchange: String,
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub quantity_mode: Option<String>,
    #[serde(default = "default_quantity")]
    pub quantity: f64,
    #[serde(default = "default_side")]
    pub side: String,
    #[serde(default = "default_order_type")]
    pub order_type: String,
    #[serde(default)]
    pub price: Option<f64>,
    #[serde(default)]
    pub leverage: Option<i64>,
    #[serde(default)]
    pub scheduled_at: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub cron: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigFile {
    #[serde(default)]
    pub tasks: Vec<TaskItem>,
    #[serde(default)]
    pub strategy_tasks: Vec<StrategyTask>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

impl Default for ConfigFile {
    fn default() -> Self {
        Self {
            tasks: vec![],
            strategy_tasks: vec![],
            enabled: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StrategyTask {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default = "default_strategy_kind")]
    pub strategy_kind: String,
    #[serde(default = "default_exchange")]
    pub exchange: String,
    #[serde(default)]
    pub symbol: String,
    #[serde(default = "default_quantity")]
    pub amount: f64,
    #[serde(default)]
    pub leverage: Option<i64>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantityResolveRequest {
    #[serde(default = "default_exchange")]
    pub exchange: String,
    #[serde(default = "default_symbol")]
    pub symbol: String,
    #[serde(default)]
    pub quantity_mode: Option<String>,
    #[serde(default = "default_quantity")]
    pub quantity: f64,
    #[serde(default)]
    pub leverage: Option<i64>,
    #[serde(default)]
    pub price: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResolvedQuantity {
    pub quantity: f64,
    pub input_quantity: f64,
    pub input_mode: String,
    pub reference_price: Option<f64>,
    pub leverage_used: Option<i64>,
    pub raw_quantity: Option<f64>,
    pub notional: Option<f64>,
    pub contract_multiplier: Option<f64>,
    pub human_quantity: Option<f64>,
    pub market_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveQuantityResponse {
    pub exchange: String,
    pub symbol: String,
    pub quantity_mode: String,
    pub input_mode: String,
    pub input_quantity: f64,
    pub resolved_quantity: f64,
    pub display_quantity: f64,
    pub reference_price: Option<f64>,
    pub leverage_used: Option<i64>,
    pub raw_quantity: Option<f64>,
    pub notional: Option<f64>,
    pub contract_multiplier: Option<f64>,
    pub human_quantity: Option<f64>,
    pub market_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResult {
    pub success: bool,
    #[serde(default)]
    pub order_id: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractPayload {
    pub exchange: String,
    pub symbols: Vec<String>,
    pub count: usize,
    pub cached: bool,
    pub stale: bool,
    pub updated_at: String,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiErrorBody {
    pub detail: String,
}

pub fn default_exchange() -> String {
    "binance".to_string()
}

pub fn default_symbol() -> String {
    "BTCUSDT".to_string()
}

pub fn default_quantity() -> f64 {
    10.0
}

pub fn default_side() -> String {
    "long".to_string()
}

pub fn default_order_type() -> String {
    "market".to_string()
}

pub fn default_enabled() -> bool {
    true
}

pub fn default_strategy_kind() -> String {
    "minute_drop_short".to_string()
}

pub fn normalize_exchange(exchange: Option<&str>, strict: bool) -> anyhow::Result<String> {
    let value = exchange.unwrap_or("binance").trim().to_lowercase();
    if SUPPORTED_EXCHANGES.contains(&value.as_str()) {
        Ok(value)
    } else if strict {
        Err(anyhow::anyhow!("Unsupported exchange: {}", exchange.unwrap_or("")))
    } else {
        Ok("binance".to_string())
    }
}

pub fn normalize_quantity_mode(input: Option<&str>, default_mode: &str) -> String {
    match input.unwrap_or("").trim().to_lowercase().as_str() {
        "margin" => "margin".to_string(),
        "contract" => "contract".to_string(),
        _ => default_mode.to_string(),
    }
}

pub fn default_symbol_for_exchange(exchange: &str) -> String {
    match exchange {
        "gateio" => "BTC_USDT".to_string(),
        "okx" => "BTC-USDT-SWAP".to_string(),
        _ => "BTCUSDT".to_string(),
    }
}

pub fn default_quantity_for_exchange(exchange: &str, quantity_mode: &str) -> f64 {
    if quantity_mode == "margin" {
        return 10.0;
    }
    match exchange {
        "gateio" => 1.0,
        "okx" => 0.01,
        _ => 0.001,
    }
}

pub fn normalize_symbol(symbol: Option<&str>, exchange: &str) -> String {
    let normalized = symbol
        .unwrap_or("")
        .trim()
        .to_uppercase()
        .replace(' ', "");
    if normalized.is_empty() {
        default_symbol_for_exchange(exchange)
    } else {
        normalized
    }
}

pub fn parse_scheduled_at(value: Option<&str>) -> Option<DateTime<Local>> {
    let raw = value?.trim();
    if raw.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Local))
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(&raw.replace(' ', "T"), "%Y-%m-%dT%H:%M:%S")
                .ok()
                .and_then(|naive| Local.from_local_datetime(&naive).single())
        })
}

pub fn parse_legacy_cron(value: Option<&str>) -> Option<DateTime<Local>> {
    let cron = value?.trim();
    if cron.is_empty() || cron.contains(' ') {
        return None;
    }
    let parts: Vec<_> = cron.split(':').collect();
    if parts.len() != 2 && parts.len() != 3 {
        return None;
    }
    let hour = parts[0].parse::<u32>().ok()?;
    let minute = parts[1].parse::<u32>().ok()?;
    let second = if parts.len() == 3 {
        parts[2].parse::<u32>().ok()?
    } else {
        0
    };
    let now = Local::now();
    let mut candidate = now
        .with_hour(hour)?
        .with_minute(minute)?
        .with_second(second)?
        .with_nanosecond(0)?;
    if candidate < now {
        candidate += chrono::Duration::days(1);
    }
    Some(candidate)
}

pub fn format_scheduled_at(value: Option<DateTime<Local>>) -> String {
    value
        .map(|dt| dt.with_nanosecond(0).unwrap_or(dt).format("%Y-%m-%dT%H:%M:%S").to_string())
        .unwrap_or_default()
}

pub fn normalize_strategy_task(task: &StrategyTask) -> anyhow::Result<StrategyTask> {
    let exchange = normalize_exchange(Some(&task.exchange), false)?;
    let strategy_kind = match task.strategy_kind.trim().to_lowercase().as_str() {
        "minute_drop_short" | "" => "minute_drop_short".to_string(),
        other => return Err(anyhow::anyhow!("Unsupported strategy kind: {}", other)),
    };
    let amount = if task.amount > 0.0 {
        task.amount
    } else {
        default_quantity()
    };
    let symbol = normalize_symbol(Some(&task.symbol), &exchange);
    let id = if task.id.trim().is_empty() {
        format!("{}:{}:{}", strategy_kind, exchange, symbol)
    } else {
        task.id.trim().to_string()
    };
    Ok(StrategyTask {
        id,
        name: task.name.trim().to_string(),
        strategy_kind,
        exchange: exchange.clone(),
        symbol,
        amount,
        leverage: task.leverage.map(|value| value.max(1)),
        enabled: task.enabled,
    })
}

pub fn normalize_task(task: &TaskItem) -> anyhow::Result<TaskItem> {
    let exchange = normalize_exchange(Some(&task.exchange), false)?;
    let quantity_mode = if task.quantity_mode.is_some() {
        normalize_quantity_mode(task.quantity_mode.as_deref(), "margin")
    } else {
        "contract".to_string()
    };
    let default_quantity = default_quantity_for_exchange(&exchange, &quantity_mode);
    let scheduled_at = parse_scheduled_at(Some(&task.scheduled_at))
        .or_else(|| parse_legacy_cron(task.cron.as_deref()));
    Ok(TaskItem {
        name: task.name.clone(),
        exchange: exchange.clone(),
        symbol: normalize_symbol(Some(&task.symbol), &exchange),
        quantity_mode: Some(quantity_mode),
        quantity: if task.quantity > 0.0 { task.quantity } else { default_quantity },
        side: if task.side.eq_ignore_ascii_case("short") {
            "short".to_string()
        } else {
            "long".to_string()
        },
        order_type: if task.order_type.eq_ignore_ascii_case("limit") {
            "limit".to_string()
        } else {
            "market".to_string()
        },
        price: task.price,
        leverage: task.leverage,
        scheduled_at: format_scheduled_at(scheduled_at),
        enabled: task.enabled,
        cron: None,
    })
}
