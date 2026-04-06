use std::collections::BTreeMap;

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;

use crate::exchanges::{
    normalize_contract_quantity, resolve_margin_quantity, ExchangeClient, PlaceOrderRequest,
};
use crate::models::{OrderResult, ResolvedQuantity};

type HmacSha256 = Hmac<Sha256>;

pub struct BinanceClient {
    client: Client,
    api_key: String,
    api_secret: String,
    base_url: String,
}

impl BinanceClient {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            api_key: std::env::var("BINANCE_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("BINANCE_API_SECRET").unwrap_or_default(),
            base_url: "https://fapi.binance.com".to_string(),
        }
    }

    fn to_contract_symbol(&self, symbol: &str) -> String {
        let mut normalized = symbol.replace('_', "").replace('-', "").to_uppercase();
        if !normalized.ends_with("USDT") {
            normalized.push_str("USDT");
        }
        normalized
    }

    async fn symbol_info(&self, symbol: &str) -> anyhow::Result<Value> {
        let payload = self
            .client
            .get(format!("{}/fapi/v1/exchangeInfo", self.base_url))
            .query(&[("symbol", symbol)])
            .send()
            .await?
            .json::<Value>()
            .await?;
        let symbols = payload["symbols"].as_array().cloned().unwrap_or_default();
        symbols
            .into_iter()
            .find(|item| item["symbol"].as_str().unwrap_or_default().eq_ignore_ascii_case(symbol))
            .ok_or_else(|| anyhow::anyhow!("Binance symbol info not found for {}", symbol))
    }

    async fn reference_price(&self, symbol: &str, price: Option<f64>) -> anyhow::Result<f64> {
        if let Some(px) = price {
            if px > 0.0 {
                return Ok(px);
            }
        }
        let payload = self
            .client
            .get(format!("{}/fapi/v1/premiumIndex", self.base_url))
            .query(&[("symbol", symbol)])
            .send()
            .await?
            .json::<Value>()
            .await?;
        let mark_price = payload["markPrice"]
            .as_str()
            .or_else(|| payload["lastPrice"].as_str())
            .ok_or_else(|| anyhow::anyhow!("Binance mark price not found for {}", symbol))?;
        Ok(mark_price.parse::<f64>()?)
    }

    fn signed_query(&self, mut params: BTreeMap<String, String>) -> anyhow::Result<String> {
        params.insert(
            "timestamp".to_string(),
            chrono::Utc::now().timestamp_millis().to_string(),
        );
        let query = serde_urlencoded::to_string(&params)?;
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());
        Ok(format!("{}&signature={}", query, signature))
    }

    fn auth_ok(&self) -> bool {
        !self.api_key.is_empty() && !self.api_secret.is_empty()
    }
}

#[async_trait]
impl ExchangeClient for BinanceClient {
    async fn list_contract_symbols(&self) -> anyhow::Result<Vec<String>> {
        let payload = self
            .client
            .get(format!("{}/fapi/v1/exchangeInfo", self.base_url))
            .send()
            .await?
            .json::<Value>()
            .await?;
        let mut symbols = vec![];
        for item in payload["symbols"].as_array().cloned().unwrap_or_default() {
            if item["contractType"].as_str() == Some("PERPETUAL")
                && item["quoteAsset"].as_str() == Some("USDT")
                && item["status"].as_str() == Some("TRADING")
            {
                if let Some(symbol) = item["symbol"].as_str() {
                    symbols.push(symbol.to_string());
                }
            }
        }
        symbols.sort();
        symbols.dedup();
        Ok(symbols)
    }

    async fn resolve_order_quantity(
        &self,
        symbol: &str,
        quantity: f64,
        quantity_mode: &str,
        leverage: Option<i64>,
        price: Option<f64>,
    ) -> anyhow::Result<ResolvedQuantity> {
        let symbol = self.to_contract_symbol(symbol);
        let info = self.symbol_info(&symbol).await?;
        let filters = info["filters"].as_array().cloned().unwrap_or_default();
        let lot_filter = filters
            .iter()
            .find(|f| f["filterType"].as_str() == Some("MARKET_LOT_SIZE"))
            .or_else(|| filters.iter().find(|f| f["filterType"].as_str() == Some("LOT_SIZE")))
            .ok_or_else(|| anyhow::anyhow!("Binance lot size filter not found for {}", symbol))?;
        let min_qty = lot_filter["minQty"]
            .as_str()
            .unwrap_or("0.001")
            .parse::<f64>()?;
        let step = lot_filter["stepSize"]
            .as_str()
            .unwrap_or("0.001")
            .parse::<f64>()?;

        if quantity_mode == "margin" {
            let ref_price = self.reference_price(&symbol, price).await?;
            return resolve_margin_quantity(
                quantity,
                leverage,
                ref_price,
                min_qty,
                step,
                1.0,
                "contract quantity",
            );
        }
        let resolved = normalize_contract_quantity(quantity, min_qty, step, "contract quantity")?;
        Ok(ResolvedQuantity {
            quantity: resolved,
            input_quantity: quantity,
            input_mode: "contract".to_string(),
            reference_price: price,
            leverage_used: None,
            raw_quantity: None,
            notional: None,
            contract_multiplier: None,
            human_quantity: None,
            market_id: None,
        })
    }

    async fn place_order(&self, req: PlaceOrderRequest) -> anyhow::Result<OrderResult> {
        if !self.auth_ok() {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                message: "Missing Binance API credentials".to_string(),
            });
        }
        let symbol = self.to_contract_symbol(&req.symbol);
        let side = if req.side.eq_ignore_ascii_case("short") || req.side.eq_ignore_ascii_case("sell") {
            "SELL"
        } else {
            "BUY"
        };
        let order_type = if req.order_type.eq_ignore_ascii_case("limit") {
            "LIMIT"
        } else {
            "MARKET"
        };

        if let Some(leverage) = req.leverage {
            let mut lev_params = BTreeMap::new();
            lev_params.insert("symbol".to_string(), symbol.clone());
            lev_params.insert("leverage".to_string(), leverage.to_string());
            let lev_query = self.signed_query(lev_params)?;
            let _ = self
                .client
                .post(format!("{}/fapi/v1/leverage?{}", self.base_url, lev_query))
                .header("X-MBX-APIKEY", &self.api_key)
                .send()
                .await?;
        }

        let mut params = BTreeMap::new();
        params.insert("symbol".to_string(), symbol);
        params.insert("side".to_string(), side.to_string());
        params.insert("type".to_string(), order_type.to_string());
        params.insert("quantity".to_string(), req.quantity.to_string());
        if order_type == "LIMIT" {
            let px = req
                .price
                .ok_or_else(|| anyhow::anyhow!("Limit order requires a valid price"))?;
            params.insert("timeInForce".to_string(), "GTC".to_string());
            params.insert("price".to_string(), px.to_string());
        }
        let query = self.signed_query(params)?;
        let payload = self
            .client
            .post(format!("{}/fapi/v1/order?{}", self.base_url, query))
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?
            .json::<Value>()
            .await?;
        if let Some(order_id) = payload["orderId"].as_i64() {
            Ok(OrderResult {
                success: true,
                order_id: Some(order_id.to_string()),
                message: "Order placed successfully".to_string(),
            })
        } else {
            Ok(OrderResult {
                success: false,
                order_id: None,
                message: payload["msg"]
                    .as_str()
                    .unwrap_or("Binance order failed")
                    .to_string(),
            })
        }
    }
}

