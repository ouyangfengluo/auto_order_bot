use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::{json, Value};
use sha2::Digest;
use sha2::Sha512;

use crate::exchanges::{
    normalize_contract_quantity, resolve_margin_quantity, Candle, ExchangeClient, PlaceOrderRequest,
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

    fn parse_i64(value: &Value) -> Option<i64> {
        if let Some(v) = value.as_i64() {
            return Some(v);
        }
        if let Some(v) = value.as_u64() {
            return i64::try_from(v).ok();
        }
        value.as_str()?.parse::<i64>().ok()
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

    fn format_error_message(status: reqwest::StatusCode, raw_text: &str, parsed: Option<&Value>) -> String {
        if let Some(payload) = parsed {
            if let Some(message) = payload["message"].as_str() {
                let msg = message.trim();
                if !msg.is_empty() {
                    if let Some(label) = payload["label"].as_str() {
                        let lb = label.trim();
                        if !lb.is_empty() {
                            return format!("{} | {} (HTTP {})", msg, lb, status.as_u16());
                        }
                    }
                    return format!("{} (HTTP {})", msg, status.as_u16());
                }
            }
            if let Some(label) = payload["label"].as_str() {
                let lb = label.trim();
                if !lb.is_empty() {
                    return format!("{} (HTTP {})", lb, status.as_u16());
                }
            }
        }

        let text = raw_text.trim();
        if !text.is_empty() {
            return format!("{} (HTTP {})", text, status.as_u16());
        }

        format!("HTTP {}", status.as_u16())
    }

    fn extract_order_id(payload: &Value) -> Option<String> {
        if let Some(id) = payload["id"].as_str() {
            let trimmed = id.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
        if let Some(id) = payload["id"].as_i64() {
            return Some(id.to_string());
        }
        if let Some(id) = payload["id"].as_u64() {
            return Some(id.to_string());
        }
        None
    }

    async fn signed_request(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: &str,
    ) -> anyhow::Result<Value> {
        let headers = self.signed_headers(method, path, query, body)?;
        let url = if query.is_empty() {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}{}?{}", self.base_url, path, query)
        };
        let mut request = self.client.request(method.parse()?, url);
        for (k, v) in headers {
            request = request.header(k, v);
        }
        if !body.is_empty() {
            request = request.body(body.to_string());
        }
        Ok(request.send().await?.json::<Value>().await?)
    }

    fn parse_candle(item: &Value) -> Option<Candle> {
        Some(Candle {
            timestamp: Self::parse_i64(&item["t"])?,
            open: Self::parse_number(&item["o"])?,
            high: Self::parse_number(&item["h"])?,
            low: Self::parse_number(&item["l"])?,
            close: Self::parse_number(&item["c"])?,
        })
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
        let status = resp.status();
        let raw_text = resp.text().await.unwrap_or_default();
        let data: Option<Value> = serde_json::from_str(&raw_text).ok();
        if let Some(order_id) = data
            .as_ref()
            .and_then(Self::extract_order_id)
        {
            Ok(OrderResult {
                success: true,
                order_id: Some(order_id.to_string()),
                message: "Order placed successfully".to_string(),
            })
        } else if status.is_success() && raw_text.contains("\"id\"") {
            Ok(OrderResult {
                success: true,
                order_id: None,
                message: "Order placed successfully".to_string(),
            })
        } else {
            Ok(OrderResult {
                success: false,
                order_id: None,
                message: Self::format_error_message(status, &raw_text, data.as_ref()),
            })
        }
    }

    async fn fetch_recent_candles(&self, symbol: &str, limit: usize) -> anyhow::Result<Vec<Candle>> {
        let contract = self.to_contract_symbol(symbol);
        let payload = self
            .client
            .get(format!("{}/futures/usdt/candlesticks", self.base_url))
            .query(&[
                ("contract", contract.as_str()),
                ("interval", "1m"),
                ("limit", &limit.max(1).to_string()),
            ])
            .send()
            .await?
            .json::<Value>()
            .await?;
        let mut candles = payload
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|item| Self::parse_candle(&item))
            .collect::<Vec<_>>();
        candles.sort_by_key(|item| item.timestamp);
        Ok(candles)
    }

    async fn fetch_available_balance(&self) -> anyhow::Result<f64> {
        if !self.auth_ok() {
            return Err(anyhow::anyhow!("Missing Gate.io API credentials"));
        }
        let payload = self
            .signed_request("GET", "/futures/usdt/accounts", "", "")
            .await?;
        Self::parse_number(&payload["available"])
            .or_else(|| Self::parse_number(&payload["available_balance"]))
            .ok_or_else(|| anyhow::anyhow!("Gate.io available balance not found"))
    }

    async fn fetch_position_size(&self, symbol: &str) -> anyhow::Result<f64> {
        if !self.auth_ok() {
            return Err(anyhow::anyhow!("Missing Gate.io API credentials"));
        }
        let contract = self.to_contract_symbol(symbol);
        let payload = self
            .signed_request("GET", &format!("/futures/usdt/positions/{}", contract), "", "")
            .await?;
        Self::parse_number(&payload["size"])
            .ok_or_else(|| anyhow::anyhow!("Gate.io position size not found for {}", contract))
    }

    async fn place_reduce_only_order(&self, req: PlaceOrderRequest) -> anyhow::Result<OrderResult> {
        if !self.auth_ok() {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                message: "Missing Gate.io API credentials".to_string(),
            });
        }
        let contract = self.to_contract_symbol(&req.symbol);
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
            "reduce_only": true
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
        let data = self
            .signed_request("POST", "/futures/usdt/orders", "", &body)
            .await?;
        if let Some(order_id) = Self::extract_order_id(&data) {
            Ok(OrderResult {
                success: true,
                order_id: Some(order_id),
                message: "Order placed successfully".to_string(),
            })
        } else {
            Ok(OrderResult {
                success: false,
                order_id: None,
                message: data["message"]
                    .as_str()
                    .unwrap_or("Gate.io reduce-only order failed")
                    .to_string(),
            })
        }
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> anyhow::Result<()> {
        if !self.auth_ok() {
            return Err(anyhow::anyhow!("Missing Gate.io API credentials"));
        }
        let contract = self.to_contract_symbol(symbol);
        let query = format!("contract={}", contract);
        let _ = self
            .signed_request("DELETE", &format!("/futures/usdt/orders/{}", order_id), &query, "")
            .await?;
        Ok(())
    }
}
