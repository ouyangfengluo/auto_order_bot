use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::{json, Value};
use sha2::Digest;
use sha2::Sha512;

use crate::exchanges::{
    normalize_contract_quantity, resolve_margin_quantity, ExchangeClient, PlaceOrderRequest,
};
use crate::models::{OrderResult, ResolvedQuantity};

type HmacSha512 = Hmac<Sha512>;

pub struct GateioClient {
    client: Client,
    api_key: String,
    api_secret: String,
    base_url: String,
    prefix: String,
}

impl GateioClient {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            api_key: std::env::var("GATEIO_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("GATEIO_API_SECRET").unwrap_or_default(),
            base_url: "https://api.gateio.ws/api/v4".to_string(),
            prefix: "/api/v4".to_string(),
        }
    }

    fn to_contract_symbol(&self, symbol: &str) -> String {
        let normalized = symbol.replace('-', "").to_uppercase();
        if normalized.contains('_') {
            return normalized;
        }
        if normalized.ends_with("USDT") {
            format!("{}_USDT", &normalized[..normalized.len() - 4])
        } else {
            format!("{}_USDT", normalized)
        }
    }

    async fn contract_info(&self, contract: &str) -> anyhow::Result<Value> {
        let payload = self
            .client
            .get(format!("{}/futures/usdt/contracts/{}", self.base_url, contract))
            .send()
            .await?
            .json::<Value>()
            .await?;
        if payload["name"].as_str().unwrap_or_default().is_empty() {
            Err(anyhow::anyhow!("Gate.io contract info not found for {}", contract))
        } else {
            Ok(payload)
        }
    }

    fn parse_number(value: &Value) -> Option<f64> {
        if let Some(v) = value.as_f64() {
            return Some(v);
        }
        if let Some(v) = value.as_i64() {
            return Some(v as f64);
        }
        if let Some(v) = value.as_u64() {
            return Some(v as f64);
        }
        value.as_str()?.parse::<f64>().ok()
    }

    fn signed_headers(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: &str,
    ) -> anyhow::Result<Vec<(&'static str, String)>> {
        let ts = chrono::Utc::now().timestamp().to_string();
        let hashed_body = hex::encode(sha2::Sha512::digest(body.as_bytes()));
        let sign_string = format!(
            "{}\n{}{}\n{}\n{}\n{}",
            method,
            self.prefix,
            path,
            query,
            hashed_body,
            ts
        );
        let mut mac = HmacSha512::new_from_slice(self.api_secret.as_bytes())?;
        mac.update(sign_string.as_bytes());
        let sign = hex::encode(mac.finalize().into_bytes());
        Ok(vec![
            ("KEY", self.api_key.clone()),
            ("SIGN", sign),
            ("Timestamp", ts),
            ("Accept", "application/json".to_string()),
            ("Content-Type", "application/json".to_string()),
        ])
    }

    fn auth_ok(&self) -> bool {
        !self.api_key.is_empty() && !self.api_secret.is_empty()
    }
}

#[async_trait]
impl ExchangeClient for GateioClient {
    async fn list_contract_symbols(&self) -> anyhow::Result<Vec<String>> {
        let payload = self
            .client
            .get(format!("{}/futures/usdt/contracts", self.base_url))
            .send()
            .await?
            .json::<Value>()
            .await?;
        let mut symbols = vec![];
        for item in payload.as_array().cloned().unwrap_or_default() {
            if item["in_delisting"].as_bool() == Some(false)
                && item["status"].as_str() == Some("trading")
            {
                if let Some(name) = item["name"].as_str() {
                    symbols.push(name.to_string());
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
        let contract = self.to_contract_symbol(symbol);
        let info = self.contract_info(&contract).await?;
        let min_qty = Self::parse_number(&info["order_size_min"]).unwrap_or(1.0);
        let step = 1.0;
        let contract_multiplier = Self::parse_number(&info["quanto_multiplier"]).unwrap_or(1.0);
        if quantity_mode == "margin" {
            let reference_price = price
                .filter(|v| *v > 0.0)
                .or_else(|| Self::parse_number(&info["mark_price"]))
                .or_else(|| Self::parse_number(&info["last_price"]))
                .ok_or_else(|| anyhow::anyhow!("Gate.io mark price not found for {}", contract))?;
            return resolve_margin_quantity(
                quantity,
                leverage,
                reference_price,
                min_qty,
                step,
                contract_multiplier,
                "contract size",
            );
        }
        let resolved = normalize_contract_quantity(quantity, min_qty, step, "contract size")?;
        Ok(ResolvedQuantity {
            quantity: resolved,
            input_quantity: quantity,
            input_mode: "contract".to_string(),
            reference_price: price,
            leverage_used: None,
            raw_quantity: None,
            notional: None,
            contract_multiplier: Some(contract_multiplier),
            human_quantity: None,
            market_id: None,
        })
    }

    async fn place_order(&self, req: PlaceOrderRequest) -> anyhow::Result<OrderResult> {
        if !self.auth_ok() {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                message: "Missing Gate.io API credentials".to_string(),
            });
        }
        let contract = self.to_contract_symbol(&req.symbol);
        if let Some(leverage) = req.leverage {
            let path = format!("/futures/usdt/positions/{}/leverage", contract);
            let query = format!("leverage={}", leverage);
            let headers = self.signed_headers("POST", &path, &query, "")?;
            let mut request = self.client.post(format!("{}{}?{}", self.base_url, path, query));
            for (k, v) in headers {
                request = request.header(k, v);
            }
            let _ = request.send().await?;
        }

        let mut size = req.quantity.round() as i64;
        if req.side.eq_ignore_ascii_case("short") || req.side.eq_ignore_ascii_case("sell") {
            size = -size.abs();
        } else {
            size = size.abs();
        }
        if size == 0 {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                message: "Invalid Gate.io contract size".to_string(),
            });
        }

        let mut payload = json!({
            "contract": contract,
            "size": size,
            "reduce_only": false
        });
        if req.order_type.eq_ignore_ascii_case("limit") {
            let px = req
                .price
                .ok_or_else(|| anyhow::anyhow!("Limit order requires a valid price"))?;
            payload["price"] = Value::String(px.to_string());
            payload["tif"] = Value::String("gtc".to_string());
        } else {
            payload["price"] = Value::String("0".to_string());
            payload["tif"] = Value::String("ioc".to_string());
        }
        let body = serde_json::to_string(&payload)?;
        let path = "/futures/usdt/orders";
        let headers = self.signed_headers("POST", path, "", &body)?;
        let mut request = self.client.post(format!("{}{}", self.base_url, path)).body(body);
        for (k, v) in headers {
            request = request.header(k, v);
        }
        let resp = request.send().await?;
        let data: Value = resp.json().await?;
        if let Some(order_id) = data["id"].as_str() {
            Ok(OrderResult {
                success: true,
                order_id: Some(order_id.to_string()),
                message: "Order placed successfully".to_string(),
            })
        } else {
            Ok(OrderResult {
                success: false,
                order_id: None,
                message: data["message"]
                    .as_str()
                    .or_else(|| data["label"].as_str())
                    .unwrap_or("Gate.io order failed")
                    .to_string(),
            })
        }
    }
}
