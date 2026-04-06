use async_trait::async_trait;
use base64::Engine;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::{json, Value};
use sha2::Sha256;

use crate::exchanges::{
    normalize_contract_quantity, resolve_margin_quantity, ExchangeClient, PlaceOrderRequest,
};
use crate::models::{OrderResult, ResolvedQuantity};

type HmacSha256 = Hmac<Sha256>;

pub struct OkxClient {
    client: Client,
    api_key: String,
    api_secret: String,
    passphrase: String,
    base_url: String,
    trade_mode: String,
    simulated_trading: bool,
}

impl OkxClient {
    pub fn new() -> Self {
        let trade_mode = match std::env::var("OKX_TRADE_MODE")
            .unwrap_or_else(|_| "cross".to_string())
            .to_lowercase()
            .as_str()
        {
            "isolated" => "isolated".to_string(),
            _ => "cross".to_string(),
        };
        let simulated = std::env::var("OKX_SIMULATED_TRADING")
            .unwrap_or_else(|_| "0".to_string())
            .to_lowercase();
        Self {
            client: Client::new(),
            api_key: std::env::var("OKX_API_KEY").unwrap_or_default(),
            api_secret: std::env::var("OKX_API_SECRET").unwrap_or_default(),
            passphrase: std::env::var("OKX_API_PASSPHRASE")
                .or_else(|_| std::env::var("OKX_PASSPHRASE"))
                .unwrap_or_default(),
            base_url: std::env::var("OKX_BASE_URL").unwrap_or_else(|_| "https://www.okx.com".to_string()),
            trade_mode,
            simulated_trading: matches!(simulated.as_str(), "1" | "true" | "yes" | "on"),
        }
    }

    fn to_instrument_id(&self, symbol: &str) -> String {
        let normalized = symbol
            .trim()
            .to_uppercase()
            .replace('_', "-")
            .replace('/', "-");
        if normalized.ends_with("-SWAP") {
            return normalized;
        }
        let compact = normalized.replace('-', "");
        if compact.ends_with("USDT") {
            format!("{}-USDT-SWAP", &compact[..compact.len() - 4])
        } else if compact.is_empty() {
            "BTC-USDT-SWAP".to_string()
        } else {
            format!("{}-USDT-SWAP", compact)
        }
    }

    fn timestamp() -> String {
        chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string()
    }

    fn sign(&self, timestamp: &str, method: &str, request_path: &str, body: &str) -> anyhow::Result<String> {
        let prehash = format!("{}{}{}{}", timestamp, method.to_uppercase(), request_path, body);
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())?;
        mac.update(prehash.as_bytes());
        Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
    }

    fn auth_ok(&self) -> bool {
        !self.api_key.is_empty() && !self.api_secret.is_empty() && !self.passphrase.is_empty()
    }

    async fn request(&self, method: &str, path: &str, params: Option<Vec<(&str, String)>>, body: Option<Value>, auth: bool) -> anyhow::Result<Value> {
        let query = if let Some(items) = params {
            if items.is_empty() {
                String::new()
            } else {
                format!("?{}", serde_urlencoded::to_string(items)?)
            }
        } else {
            String::new()
        };
        let request_path = format!("{}{}", path, query);
        let body_text = body
            .as_ref()
            .map(|v| serde_json::to_string(v))
            .transpose()?
            .unwrap_or_default();

        let mut req = self
            .client
            .request(method.parse()?, format!("{}{}", self.base_url.trim_end_matches('/'), request_path));

        if auth {
            let ts = Self::timestamp();
            let sign = self.sign(&ts, method, &request_path, &body_text)?;
            req = req
                .header("OK-ACCESS-KEY", &self.api_key)
                .header("OK-ACCESS-SIGN", sign)
                .header("OK-ACCESS-TIMESTAMP", ts)
                .header("OK-ACCESS-PASSPHRASE", &self.passphrase)
                .header("Content-Type", "application/json");
            if self.simulated_trading {
                req = req.header("x-simulated-trading", "1");
            }
        }
        if !body_text.is_empty() {
            req = req.body(body_text);
        }
        Ok(req.send().await?.json::<Value>().await?)
    }

    fn require_ok(payload: &Value, default_message: &str) -> anyhow::Result<()> {
        if payload["code"].as_str() == Some("0") {
            Ok(())
        } else {
            Err(anyhow::anyhow!("{}", Self::extract_top_level_error(payload, default_message)))
        }
    }

    fn extract_top_level_error(payload: &Value, default_message: &str) -> String {
        let code = payload["code"].as_str().unwrap_or("");
        let top_msg = payload["msg"].as_str().unwrap_or("").trim();

        let data_row = payload["data"].as_array().and_then(|arr| arr.first());
        let detail = data_row
            .and_then(|row| row["sMsg"].as_str().or_else(|| row["msg"].as_str()))
            .map(str::trim)
            .filter(|msg| !msg.is_empty())
            .unwrap_or("");

        let base = if !detail.is_empty() {
            if !top_msg.is_empty() && !top_msg.eq_ignore_ascii_case(detail) {
                format!("{} | {}", detail, top_msg)
            } else {
                detail.to_string()
            }
        } else if !top_msg.is_empty() {
            top_msg.to_string()
        } else {
            default_message.to_string()
        };

        if code.is_empty() {
            base
        } else {
            format!("{} (code={})", base, code)
        }
    }

    fn extract_row_error(row: &Value) -> String {
        let s_code = row["sCode"].as_str().unwrap_or("");
        let s_msg = row["sMsg"].as_str().unwrap_or("OKX order rejected").trim();
        if s_code.is_empty() || s_code == "0" {
            s_msg.to_string()
        } else {
            format!("{} (sCode={})", s_msg, s_code)
        }
    }
}

#[async_trait]
impl ExchangeClient for OkxClient {
    async fn list_contract_symbols(&self) -> anyhow::Result<Vec<String>> {
        let payload = self
            .request(
                "GET",
                "/api/v5/public/instruments",
                Some(vec![("instType", "SWAP".to_string())]),
                None,
                false,
            )
            .await?;
        Self::require_ok(&payload, "Failed to load OKX contract list")?;
        let mut symbols = vec![];
        for item in payload["data"].as_array().cloned().unwrap_or_default() {
            if item["state"].as_str() == Some("live")
                && item["settleCcy"].as_str() == Some("USDT")
                && item["ctType"].as_str() == Some("linear")
            {
                if let Some(inst_id) = item["instId"].as_str() {
                    symbols.push(inst_id.to_string());
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
        let inst_id = self.to_instrument_id(symbol);
        let instrument_payload = self
            .request(
                "GET",
                "/api/v5/public/instruments",
                Some(vec![("instType", "SWAP".to_string()), ("instId", inst_id.clone())]),
                None,
                false,
            )
            .await?;
        Self::require_ok(&instrument_payload, "Failed to load OKX instrument")?;
        let instrument = instrument_payload["data"]
            .as_array()
            .and_then(|arr| arr.first())
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("OKX instrument info not found for {}", inst_id))?;

        let min_qty = instrument["minSz"]
            .as_str()
            .or_else(|| instrument["lotSz"].as_str())
            .unwrap_or("0.01")
            .parse::<f64>()?;
        let step = instrument["lotSz"]
            .as_str()
            .or_else(|| instrument["minSz"].as_str())
            .unwrap_or("0.01")
            .parse::<f64>()?;
        let ct_val = instrument["ctVal"].as_str().unwrap_or("1").parse::<f64>()?;

        if quantity_mode == "margin" {
            let ref_price = if let Some(px) = price.filter(|v| *v > 0.0) {
                px
            } else {
                let mark_payload = self
                    .request(
                        "GET",
                        "/api/v5/public/mark-price",
                        Some(vec![("instType", "SWAP".to_string()), ("instId", inst_id.clone())]),
                        None,
                        false,
                    )
                    .await?;
                Self::require_ok(&mark_payload, "Failed to load OKX mark price")?;
                mark_payload["data"]
                    .as_array()
                    .and_then(|arr| arr.first())
                    .and_then(|item| item["markPx"].as_str())
                    .ok_or_else(|| anyhow::anyhow!("OKX mark price missing for {}", inst_id))?
                    .parse::<f64>()?
            };

            return resolve_margin_quantity(
                quantity,
                leverage,
                ref_price,
                min_qty,
                step,
                ct_val,
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
            contract_multiplier: Some(ct_val),
            human_quantity: None,
            market_id: None,
        })
    }

    async fn place_order(&self, req: PlaceOrderRequest) -> anyhow::Result<OrderResult> {
        if !self.auth_ok() {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                message: "Missing OKX API credentials".to_string(),
            });
        }
        let inst_id = self.to_instrument_id(&req.symbol);
        if let Some(leverage) = req.leverage {
            let body = json!({
                "instId": inst_id,
                "lever": leverage.to_string(),
                "mgnMode": self.trade_mode
            });
            let _ = self
                .request("POST", "/api/v5/account/set-leverage", None, Some(body), true)
                .await?;
        }

        let order_type = if req.order_type.eq_ignore_ascii_case("limit") {
            "limit"
        } else {
            "market"
        };
        let side = if req.side.eq_ignore_ascii_case("short") || req.side.eq_ignore_ascii_case("sell") {
            "sell"
        } else {
            "buy"
        };
        let mut body = json!({
            "instId": inst_id,
            "tdMode": self.trade_mode,
            "side": side,
            "ordType": order_type,
            "sz": req.quantity.to_string()
        });
        if order_type == "limit" {
            let px = req
                .price
                .ok_or_else(|| anyhow::anyhow!("Limit order requires a valid price"))?;
            body["px"] = Value::String(px.to_string());
        }
        let payload = self
            .request("POST", "/api/v5/trade/order", None, Some(body), true)
            .await?;
        if payload["code"].as_str() != Some("0") {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                message: Self::extract_top_level_error(&payload, "OKX order failed"),
            });
        }
        let row = payload["data"].as_array().and_then(|arr| arr.first()).cloned().unwrap_or_default();
        if row["sCode"].as_str().unwrap_or("0") != "0" {
            return Ok(OrderResult {
                success: false,
                order_id: None,
                message: Self::extract_row_error(&row),
            });
        }
        Ok(OrderResult {
            success: true,
            order_id: row["ordId"].as_str().map(|v| v.to_string()),
            message: "Order placed successfully".to_string(),
        })
    }
}
