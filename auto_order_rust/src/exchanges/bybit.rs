use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::{json, Value};
use sha2::Sha256;
use uuid::Uuid;

use crate::exchanges::{
    normalize_contract_quantity, resolve_margin_quantity, ExchangeClient, PlaceOrderRequest,
};
use crate::models::{OrderResult, ResolvedQuantity};

type HmacSha256 = Hmac<Sha256>;

pub struct BybitClient {
    client: Client,
    api_key: String,
    api_secret: String,
    base_url: String,
}

impl BybitClient {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            api_key: std::env::var("BYBIT_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("BYBIT_API_SECRET").unwrap_or_default(),
            base_url: "https://api.bybit.com".to_string(),
        }
    }

    fn to_symbol(&self, symbol: &str) -> String {
        let mut normalized = symbol.replace('_', "").replace('-', "").to_uppercase();
        if !normalized.ends_with("USDT") {
            normalized.push_str("USDT");
        }
        normalized
    }

    fn signed_headers(&self, body: &str) -> anyhow::Result<Vec<(&'static str, String)>> {
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        let recv_window = "5000".to_string();
        let sign_payload = format!("{}{}{}{}", timestamp, self.api_key, recv_window, body);
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())?;
        mac.update(sign_payload.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());
        Ok(vec![
            ("X-BAPI-API-KEY", self.api_key.clone()),
            ("X-BAPI-SIGN", signature),
            ("X-BAPI-TIMESTAMP", timestamp),
            ("X-BAPI-RECV-WINDOW", recv_window),
            ("X-BAPI-SIGN-TYPE", "2".to_string()),
            ("Content-Type", "application/json".to_string()),
        ])
    }

    fn auth_ok(&self) -> bool {
        !self.api_key.is_empty() && !self.api_secret.is_empty()
    }
}

#[async_trait]
impl ExchangeClient for BybitClient {
    async fn list_contract_symbols(&self) -> anyhow::Result<Vec<String>> {
        let payload = self
            .client
            .get(format!("{}/v5/market/instruments-info", self.base_url))
            .query(&[("category", "linear"), ("limit", "1000")])
            .send()
            .await?
            .json::<Value>()
            .await?;
        if payload["retCode"].as_i64().unwrap_or(-1) != 0 {
            return Err(anyhow::anyhow!(
                "{}",
                payload["retMsg"].as_str().unwrap_or("Bybit request failed")
            ));
        }
        let mut symbols = vec![];
        for item in payload["result"]["list"].as_array().cloned().unwrap_or_default() {
            if item["contractType"].as_str() == Some("LinearPerpetual")
                && item["settleCoin"].as_str() == Some("USDT")
                && item["status"].as_str() == Some("Trading")
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
        let symbol = self.to_symbol(symbol);
        let instrument_payload = self
            .client
            .get(format!("{}/v5/market/instruments-info", self.base_url))
            .query(&[("category", "linear"), ("symbol", symbol.as_str())])
            .send()
            .await?
            .json::<Value>()
            .await?;
        if instrument_payload["retCode"].as_i64().unwrap_or(-1) != 0 {
            return Err(anyhow::anyhow!(
                "{}",
                instrument_payload["retMsg"]
                    .as_str()
                    .unwrap_or("Bybit request failed")
            ));
        }
        let instrument = instrument_payload["result"]["list"]
            .as_array()
            .and_then(|arr| arr.first())
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Bybit symbol info not found for {}", symbol))?;
        let min_qty = instrument["lotSizeFilter"]["minOrderQty"]
            .as_str()
            .unwrap_or("0.001")
            .parse::<f64>()?;
        let step = instrument["lotSizeFilter"]["qtyStep"]
            .as_str()
            .unwrap_or("0.001")
            .parse::<f64>()?;
        if quantity_mode == "margin" {
            let ref_price = if let Some(px) = price.filter(|v| *v > 0.0) {
                px
            } else {
                let ticker_payload = self
                    .client
                    .get(format!("{}/v5/market/tickers", self.base_url))
                    .query(&[("category", "linear"), ("symbol", symbol.as_str())])
                    .send()
                    .await?
                    .json::<Value>()
                    .await?;
                if ticker_payload["retCode"].as_i64().unwrap_or(-1) != 0 {
                    return Err(anyhow::anyhow!(
                        "{}",
                        ticker_payload["retMsg"]
                            .as_str()
                            .unwrap_or("Bybit request failed")
                    ));
                }
                let ticker = ticker_payload["result"]["list"]
                    .as_array()
                    .and_then(|arr| arr.first())
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Bybit ticker not found for {}", symbol))?;
                ticker["markPrice"]
                    .as_str()
                    .or_else(|| ticker["lastPrice"].as_str())
                    .ok_or_else(|| anyhow::anyhow!("Bybit mark price not found for {}", symbol))?
                    .parse::<f64>()?
            };
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
                message: "Missing Bybit API credentials".to_string(),
            });
        }
        let symbol = self.to_symbol(&req.symbol);
        if let Some(leverage) = req.leverage {
            let payload = json!({
                "category": "linear",
                "symbol": symbol,
                "buyLeverage": leverage.to_string(),
                "sellLeverage": leverage.to_string()
            });
            let body = serde_json::to_string(&payload)?;
            let headers = self.signed_headers(&body)?;
            let mut request = self
                .client
                .post(format!("{}/v5/position/set-leverage", self.base_url))
                .body(body);
            for (k, v) in headers {
                request = request.header(k, v);
            }
            let _ = request.send().await?;
        }
        let order_type = if req.order_type.eq_ignore_ascii_case("limit") {
            "Limit"
        } else {
            "Market"
        };
        let side = if req.side.eq_ignore_ascii_case("short") || req.side.eq_ignore_ascii_case("sell") {
            "Sell"
        } else {
            "Buy"
        };
        let mut payload = json!({
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": req.quantity.to_string(),
            "orderLinkId": Uuid::new_v4().to_string()
        });
        if order_type == "Limit" {
            let px = req
                .price
                .ok_or_else(|| anyhow::anyhow!("Limit order requires a valid price"))?;
            payload["price"] = Value::String(px.to_string());
            payload["timeInForce"] = Value::String("GTC".to_string());
        }
        let body = serde_json::to_string(&payload)?;
        let headers = self.signed_headers(&body)?;
        let mut request = self
            .client
            .post(format!("{}/v5/order/create", self.base_url))
            .body(body);
        for (k, v) in headers {
            request = request.header(k, v);
        }
        let data = request.send().await?.json::<Value>().await?;
        if data["retCode"].as_i64().unwrap_or(-1) == 0 {
            Ok(OrderResult {
                success: true,
                order_id: data["result"]["orderId"].as_str().map(|v| v.to_string()),
                message: "Order placed successfully".to_string(),
            })
        } else {
            Ok(OrderResult {
                success: false,
                order_id: None,
                message: data["retMsg"]
                    .as_str()
                    .unwrap_or("Bybit order failed")
                    .to_string(),
            })
        }
    }
}

