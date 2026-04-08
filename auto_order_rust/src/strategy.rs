use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use chrono::{DateTime, Duration, Local};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::{
    exchanges::{ExchangeClient, PlaceOrderRequest},
    models::StrategyTask,
};

#[derive(Debug, Clone)]
pub struct ActiveTrade {
    pub opened_at: DateTime<Local>,
    pub quantity: f64,
    pub take_profit_order_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct StrategyRuntimeState {
    pub use_close_price: Option<f64>,
    pub reference_minute: Option<i64>,
    pub last_checked_minute: Option<i64>,
    pub active_trade: Option<ActiveTrade>,
}

pub type StrategyRuntimeStore = Arc<Mutex<HashMap<String, StrategyRuntimeState>>>;

pub async fn tick_strategies(
    tasks: &[StrategyTask],
    clients: &HashMap<String, Arc<dyn ExchangeClient>>,
    runtime_store: &StrategyRuntimeStore,
) -> anyhow::Result<()> {
    for task in tasks {
        if !task.enabled {
            continue;
        }
        if let Err(err) = process_strategy(task, clients, runtime_store).await {
            warn!("strategy {} failed: {}", strategy_label(task), err);
        }
    }
    Ok(())
}

async fn process_strategy(
    task: &StrategyTask,
    clients: &HashMap<String, Arc<dyn ExchangeClient>>,
    runtime_store: &StrategyRuntimeStore,
) -> anyhow::Result<()> {
    let client = clients
        .get(&task.exchange)
        .ok_or_else(|| anyhow::anyhow!("Unsupported exchange: {}", task.exchange))?;

    ensure_reference_price(task, client.as_ref(), runtime_store).await?;

    let use_close_price = {
        let runtime = runtime_store.lock().await;
        runtime
            .get(&task.id)
            .and_then(|state| state.use_close_price)
            .ok_or_else(|| anyhow::anyhow!("Missing strategy reference price"))?
    };

    let position_size = client.fetch_position_size(&task.symbol).await?;
    recover_existing_position(task, client.as_ref(), runtime_store, position_size, use_close_price).await?;

    if position_size >= 0.0 {
        let mut runtime = runtime_store.lock().await;
        if let Some(state) = runtime.get_mut(&task.id) {
            state.active_trade = None;
        }
    }

    if handle_existing_trade(task, client.as_ref(), runtime_store, position_size).await? {
        return Ok(());
    }

    let last_closed_candle = latest_closed_candle(task, client.as_ref()).await?;
    {
        let mut runtime = runtime_store.lock().await;
        let state = runtime.entry(task.id.clone()).or_default();
        if state.last_checked_minute == Some(last_closed_candle.timestamp) {
            return Ok(());
        }
        state.last_checked_minute = Some(last_closed_candle.timestamp);
    }

    if last_closed_candle.close >= last_closed_candle.open {
        return Ok(());
    }

    let available_balance = client.fetch_available_balance().await?;
    let amount = task.amount.min(available_balance);
    if amount <= 0.0 {
        return Err(anyhow::anyhow!("Available balance is 0, cannot open short"));
    }

    let resolved = client
        .resolve_order_quantity(&task.symbol, amount, "margin", task.leverage, None)
        .await
        .with_context(|| format!("failed to resolve quantity for {}", strategy_label(task)))?;

    let entry = client
        .place_order(PlaceOrderRequest {
            symbol: task.symbol.clone(),
            side: "short".to_string(),
            quantity: resolved.quantity,
            price: None,
            order_type: "market".to_string(),
            leverage: task.leverage,
        })
        .await?;

    if !entry.success {
        return Err(anyhow::anyhow!("entry order failed: {}", entry.message));
    }

    let live_position = client.fetch_position_size(&task.symbol).await?;
    let short_quantity = live_position.abs().max(resolved.quantity);
    if live_position >= 0.0 || short_quantity <= 0.0 {
        return Err(anyhow::anyhow!("short position was not established"));
    }

    let take_profit = client
        .place_reduce_only_order(PlaceOrderRequest {
            symbol: task.symbol.clone(),
            side: "long".to_string(),
            quantity: short_quantity,
            price: Some(use_close_price),
            order_type: "limit".to_string(),
            leverage: None,
        })
        .await?;

    if !take_profit.success {
        let _ = client
            .place_reduce_only_order(PlaceOrderRequest {
                symbol: task.symbol.clone(),
                side: "long".to_string(),
                quantity: short_quantity,
                price: None,
                order_type: "market".to_string(),
                leverage: None,
            })
            .await;
        return Err(anyhow::anyhow!(
            "take-profit order failed after entry: {}",
            take_profit.message
        ));
    }

    {
        let mut runtime = runtime_store.lock().await;
        let state = runtime.entry(task.id.clone()).or_default();
        state.active_trade = Some(ActiveTrade {
            opened_at: Local::now(),
            quantity: short_quantity,
            take_profit_order_id: take_profit.order_id.clone(),
        });
    }

    info!(
        "strategy {} opened short: quantity={}, use_close_price={}",
        strategy_label(task),
        short_quantity,
        use_close_price
    );

    Ok(())
}

async fn ensure_reference_price(
    task: &StrategyTask,
    client: &dyn ExchangeClient,
    runtime_store: &StrategyRuntimeStore,
) -> anyhow::Result<()> {
    {
        let runtime = runtime_store.lock().await;
        if runtime
            .get(&task.id)
            .and_then(|state| state.use_close_price)
            .is_some()
        {
            return Ok(());
        }
    }

    let now_minute = current_minute_start();
    let candles = client.fetch_recent_candles(&task.symbol, 6).await?;
    let closed = candles
        .into_iter()
        .filter(|item| item.timestamp < now_minute)
        .collect::<Vec<_>>();
    if closed.len() < 3 {
        return Err(anyhow::anyhow!("Not enough 1m candles to initialize strategy"));
    }
    let recent = &closed[closed.len() - 3..];
    let best = recent
        .iter()
        .max_by(|left, right| {
            growth_rate(left)
                .partial_cmp(&growth_rate(right))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to choose reference candle"))?;
    let use_close_price = (best.high - best.low) / 2.0;
    if use_close_price <= 0.0 {
        return Err(anyhow::anyhow!("Computed use_close_price is invalid"));
    }

    let mut runtime = runtime_store.lock().await;
    let state = runtime.entry(task.id.clone()).or_default();
    state.use_close_price = Some(use_close_price);
    state.reference_minute = Some(best.timestamp);
    info!(
        "strategy {} initialized use_close_price={} from candle={}",
        strategy_label(task),
        use_close_price,
        best.timestamp
    );
    Ok(())
}

async fn latest_closed_candle(
    task: &StrategyTask,
    client: &dyn ExchangeClient,
) -> anyhow::Result<crate::exchanges::Candle> {
    let now_minute = current_minute_start();
    client
        .fetch_recent_candles(&task.symbol, 4)
        .await?
        .into_iter()
        .filter(|item| item.timestamp < now_minute)
        .max_by_key(|item| item.timestamp)
        .ok_or_else(|| anyhow::anyhow!("No closed 1m candle available for {}", task.symbol))
}

async fn recover_existing_position(
    task: &StrategyTask,
    client: &dyn ExchangeClient,
    runtime_store: &StrategyRuntimeStore,
    position_size: f64,
    use_close_price: f64,
) -> anyhow::Result<()> {
    if position_size >= 0.0 {
        return Ok(());
    }

    let has_active_trade = {
        let runtime = runtime_store.lock().await;
        runtime
            .get(&task.id)
            .and_then(|state| state.active_trade.as_ref())
            .is_some()
    };
    if has_active_trade {
        return Ok(());
    }

    let quantity = position_size.abs();
    let tp = client
        .place_reduce_only_order(PlaceOrderRequest {
            symbol: task.symbol.clone(),
            side: "long".to_string(),
            quantity,
            price: Some(use_close_price),
            order_type: "limit".to_string(),
            leverage: None,
        })
        .await?;
    if !tp.success {
        return Err(anyhow::anyhow!(
            "found existing short position but failed to place take-profit: {}",
            tp.message
        ));
    }

    let mut runtime = runtime_store.lock().await;
    let state = runtime.entry(task.id.clone()).or_default();
    state.active_trade = Some(ActiveTrade {
        opened_at: Local::now(),
        quantity,
        take_profit_order_id: tp.order_id,
    });
    Ok(())
}

async fn handle_existing_trade(
    task: &StrategyTask,
    client: &dyn ExchangeClient,
    runtime_store: &StrategyRuntimeStore,
    position_size: f64,
) -> anyhow::Result<bool> {
    let active_trade = {
        let runtime = runtime_store.lock().await;
        runtime
            .get(&task.id)
            .and_then(|state| state.active_trade.clone())
    };

    let Some(active_trade) = active_trade else {
        return Ok(false);
    };

    if position_size >= 0.0 {
        let mut runtime = runtime_store.lock().await;
        if let Some(state) = runtime.get_mut(&task.id) {
            state.active_trade = None;
        }
        return Ok(true);
    }

    if Local::now() < active_trade.opened_at + Duration::minutes(10) {
        return Ok(true);
    }

    if let Some(order_id) = active_trade.take_profit_order_id.as_deref() {
        let _ = client.cancel_order(&task.symbol, order_id).await;
    }

    let close = client
        .place_reduce_only_order(PlaceOrderRequest {
            symbol: task.symbol.clone(),
            side: "long".to_string(),
            quantity: position_size.abs().max(active_trade.quantity),
            price: None,
            order_type: "market".to_string(),
            leverage: None,
        })
        .await?;

    if !close.success {
        return Err(anyhow::anyhow!("failed to force close timed out trade: {}", close.message));
    }

    let mut runtime = runtime_store.lock().await;
    if let Some(state) = runtime.get_mut(&task.id) {
        state.active_trade = None;
    }
    info!("strategy {} force-closed timed out short", strategy_label(task));
    Ok(true)
}

fn strategy_label(task: &StrategyTask) -> String {
    if task.name.is_empty() {
        format!("{}:{}:{}", task.strategy_kind, task.exchange, task.symbol)
    } else {
        task.name.clone()
    }
}

fn current_minute_start() -> i64 {
    let now = Local::now().timestamp();
    now - now.rem_euclid(60)
}

fn growth_rate(candle: &crate::exchanges::Candle) -> f64 {
    if candle.open <= 0.0 {
        return f64::MIN;
    }
    (candle.close - candle.open) / candle.open
}
