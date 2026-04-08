pub mod binance;
pub mod bybit;
pub mod gateio;
pub mod okx;

use async_trait::async_trait;

use crate::models::{OrderResult, ResolvedQuantity};

#[derive(Debug, Clone)]
pub struct PlaceOrderRequest {
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub price: Option<f64>,
    pub order_type: String,
    pub leverage: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct Candle {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

#[async_trait]
pub trait ExchangeClient: Send + Sync {
    async fn list_contract_symbols(&self) -> anyhow::Result<Vec<String>>;
    async fn resolve_order_quantity(
        &self,
        symbol: &str,
        quantity: f64,
        quantity_mode: &str,
        leverage: Option<i64>,
        price: Option<f64>,
    ) -> anyhow::Result<ResolvedQuantity>;
    async fn place_order(&self, req: PlaceOrderRequest) -> anyhow::Result<OrderResult>;
    async fn fetch_recent_candles(&self, _symbol: &str, _limit: usize) -> anyhow::Result<Vec<Candle>> {
        Err(anyhow::anyhow!("This exchange does not support candle queries"))
    }
    async fn fetch_available_balance(&self) -> anyhow::Result<f64> {
        Err(anyhow::anyhow!("This exchange does not support balance queries"))
    }
    async fn fetch_position_size(&self, _symbol: &str) -> anyhow::Result<f64> {
        Err(anyhow::anyhow!("This exchange does not support position queries"))
    }
    async fn place_reduce_only_order(&self, _req: PlaceOrderRequest) -> anyhow::Result<OrderResult> {
        Err(anyhow::anyhow!("This exchange does not support reduce-only orders"))
    }
    async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("This exchange does not support order cancellation"))
    }
}

pub fn floor_to_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return value;
    }
    (value / step).floor() * step
}

pub fn normalize_contract_quantity(
    quantity: f64,
    min_qty: f64,
    step: f64,
    quantity_label: &str,
) -> anyhow::Result<f64> {
    let resolved = floor_to_step(quantity, step);
    if resolved < min_qty || resolved <= 0.0 {
        return Err(anyhow::anyhow!(
            "{} is too small after rounding: {}. Minimum is {}.",
            quantity_label,
            resolved,
            min_qty
        ));
    }
    Ok(resolved)
}

pub fn resolve_margin_quantity(
    margin: f64,
    leverage: Option<i64>,
    reference_price: f64,
    min_qty: f64,
    step: f64,
    contract_multiplier: f64,
    quantity_label: &str,
) -> anyhow::Result<ResolvedQuantity> {
    if margin <= 0.0 {
        return Err(anyhow::anyhow!("Initial margin must be greater than 0."));
    }
    if reference_price <= 0.0 {
        return Err(anyhow::anyhow!("Reference price must be greater than 0."));
    }
    if contract_multiplier <= 0.0 {
        return Err(anyhow::anyhow!("Contract multiplier must be greater than 0."));
    }
    let lev = leverage.unwrap_or(1).max(1);
    let notional = margin * lev as f64;
    let raw_quantity = notional / reference_price / contract_multiplier;
    let resolved = floor_to_step(raw_quantity, step);
    if resolved < min_qty || resolved <= 0.0 {
        return Err(anyhow::anyhow!(
            "Initial margin is too small. Computed {} {} is below minimum {}.",
            quantity_label,
            resolved,
            min_qty
        ));
    }
    Ok(ResolvedQuantity {
        quantity: resolved,
        input_quantity: margin,
        input_mode: "margin".to_string(),
        reference_price: Some(reference_price),
        leverage_used: Some(lev),
        raw_quantity: Some(raw_quantity),
        notional: Some(notional),
        contract_multiplier: Some(contract_multiplier),
        human_quantity: None,
        market_id: None,
    })
}
