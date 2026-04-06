mod config;
mod exchanges;
mod models;

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Local};
use tokio::sync::Mutex;
use tower_http::{
    cors::{Any, CorsLayer},
    services::ServeDir,
};
use tracing::{error, info, warn};

use crate::config::{default_config_path, load_config, save_config};
use crate::exchanges::{
    binance::BinanceClient, bybit::BybitClient, gateio::GateioClient, okx::OkxClient, ExchangeClient,
    PlaceOrderRequest,
};
use crate::models::{
    normalize_exchange, normalize_quantity_mode, normalize_symbol, normalize_task, parse_scheduled_at,
    ApiErrorBody, ConfigFile, ContractPayload, OrderResult, QuantityResolveRequest, ResolveQuantityResponse,
    SUPPORTED_EXCHANGES, TaskItem,
};

#[derive(Clone)]
struct CachedSymbols {
    symbols: Vec<String>,
    updated_at: DateTime<Local>,
    cached_at: Instant,
}

#[derive(Clone)]
struct AppState {
    config_path: PathBuf,
    static_dir: PathBuf,
    clients: HashMap<String, Arc<dyn ExchangeClient>>,
    contract_cache: Arc<Mutex<HashMap<String, CachedSymbols>>>,
    scheduler_lock: Arc<Mutex<()>>,
}

#[derive(serde::Deserialize)]
struct ContractsQuery {
    exchange: String,
    #[serde(default)]
    refresh: bool,
}

#[derive(serde::Deserialize)]
struct BalanceQuery {
    exchange: String,
}

#[derive(serde::Deserialize)]
struct OrderStatusQuery {
    exchange: String,
    order_id: String,
    symbol: Option<String>,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiErrorBody>)>;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    if std::env::var("AUTO_ORDER_CONFIG_PATH").is_err() {
        let parent_env = PathBuf::from("..").join(".env");
        if parent_env.exists() {
            let _ = dotenvy::from_path(parent_env);
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,auto_order_rust=info".into()),
        )
        .init();

    let config_path = default_config_path();
    let static_dir = std::env::var("AUTO_ORDER_STATIC_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("static"));
    let state = AppState {
        config_path,
        static_dir: static_dir.clone(),
        clients: build_clients(),
        contract_cache: Arc::new(Mutex::new(HashMap::new())),
        scheduler_lock: Arc::new(Mutex::new(())),
    };

    let scheduler_state = state.clone();
    tokio::spawn(async move {
        scheduler_loop(scheduler_state).await;
    });

    let app = Router::new()
        .route("/", get(index))
        .route("/api/config", get(get_config).post(update_config))
        .route("/api/execute", post(execute_now))
        .route("/api/resolve-quantity", post(resolve_quantity))
        .route("/api/exchanges", get(list_exchanges))
        .route("/api/contracts", get(list_contracts))
        .route("/api/balance", get(get_balance))
        .route("/api/order-status", get(get_order_status))
        .nest_service("/static", ServeDir::new(static_dir))
        .with_state(state)
        .layer(CorsLayer::new().allow_headers(Any).allow_methods(Any).allow_origin(Any));

    let host = std::env::var("AUTO_ORDER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("AUTO_ORDER_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(2888);
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", host, port))
        .await
        .expect("failed to bind server");
    info!("auto_order_rust listening on http://{}:{}", host, port);
    axum::serve(listener, app).await.expect("server failed");
}

fn build_clients() -> HashMap<String, Arc<dyn ExchangeClient>> {
    let mut map: HashMap<String, Arc<dyn ExchangeClient>> = HashMap::new();
    map.insert("binance".to_string(), Arc::new(BinanceClient::new()));
    map.insert("gateio".to_string(), Arc::new(GateioClient::new()));
    map.insert("bybit".to_string(), Arc::new(BybitClient::new()));
    map.insert("okx".to_string(), Arc::new(OkxClient::new()));
    map
}

async fn scheduler_loop(state: AppState) {
    loop {
        if let Err(err) = tick_scheduler(&state).await {
            warn!("scheduler tick failed: {}", err);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn tick_scheduler(state: &AppState) -> anyhow::Result<()> {
    let _guard = state.scheduler_lock.lock().await;
    let mut config = load_config(&state.config_path).await;
    if !config.enabled {
        return Ok(());
    }

    let now = Local::now();
    let mut changed = false;
    for task in config.tasks.iter_mut() {
        if !task.enabled {
            continue;
        }
        let scheduled = parse_scheduled_at(Some(&task.scheduled_at));
        let Some(scheduled_at) = scheduled else {
            continue;
        };
        if scheduled_at <= now {
            let normalized = normalize_task(task)?;
            let result = run_task(state, &normalized).await;
            info!(
                "scheduled task executed: {} -> success={}, message={}",
                normalized.name, result.success, result.message
            );
            task.enabled = false;
            changed = true;
        }
    }
    if changed {
        save_config(&state.config_path, &config).await?;
    }
    Ok(())
}

async fn run_task(state: &AppState, task: &TaskItem) -> OrderResult {
    match resolve_order_quantity_for_task(state, task, "contract").await {
        Ok(payload) => {
            let client = match state.clients.get(&payload.exchange) {
                Some(client) => client,
                None => {
                    return OrderResult {
                        success: false,
                        order_id: None,
                        message: format!("Unsupported exchange: {}", payload.exchange),
                    }
                }
            };
            let result = client
                .place_order(PlaceOrderRequest {
                    symbol: payload.symbol,
                    side: task.side.clone(),
                    quantity: payload.resolved_quantity,
                    price: task.price,
                    order_type: task.order_type.clone(),
                    leverage: task.leverage,
                })
                .await;
            result.unwrap_or_else(|err| OrderResult {
                success: false,
                order_id: None,
                message: err.to_string(),
            })
        }
        Err(err) => OrderResult {
            success: false,
            order_id: None,
            message: err.to_string(),
        },
    }
}

async fn resolve_order_quantity_for_task(
    state: &AppState,
    task: &TaskItem,
    default_quantity_mode: &str,
) -> anyhow::Result<ResolveQuantityResponse> {
    let exchange = normalize_exchange(Some(&task.exchange), true)?;
    let quantity_mode = normalize_quantity_mode(task.quantity_mode.as_deref(), default_quantity_mode);
    let symbol = normalize_symbol(Some(&task.symbol), &exchange);
    let client = state
        .clients
        .get(&exchange)
        .ok_or_else(|| anyhow::anyhow!("Unsupported exchange: {}", exchange))?;
    let resolved = client
        .resolve_order_quantity(
            &symbol,
            task.quantity,
            &quantity_mode,
            task.leverage,
            task.price,
        )
        .await?;
    let display_quantity = resolved.human_quantity.unwrap_or(resolved.quantity);
    Ok(ResolveQuantityResponse {
        exchange,
        symbol,
        quantity_mode,
        input_mode: resolved.input_mode.clone(),
        input_quantity: task.quantity,
        resolved_quantity: resolved.quantity,
        display_quantity,
        reference_price: resolved.reference_price,
        leverage_used: resolved.leverage_used.or(task.leverage),
        raw_quantity: resolved.raw_quantity,
        notional: resolved.notional,
        contract_multiplier: resolved.contract_multiplier,
        human_quantity: resolved.human_quantity,
        market_id: resolved.market_id,
    })
}

async fn index(State(state): State<AppState>) -> impl IntoResponse {
    let html_path = state.static_dir.join("index.html");
    match tokio::fs::read_to_string(html_path).await {
        Ok(text) => Html(text).into_response(),
        Err(_) => Html("<h1>auto_order_rust is running</h1>".to_string()).into_response(),
    }
}

async fn get_config(State(state): State<AppState>) -> ApiResult<ConfigFile> {
    let config = load_config(&state.config_path).await;
    Ok(Json(config))
}

async fn update_config(State(state): State<AppState>, Json(mut body): Json<ConfigFile>) -> ApiResult<serde_json::Value> {
    let mut normalized_tasks = vec![];
    for task in &body.tasks {
        match normalize_task(task) {
            Ok(item) => normalized_tasks.push(item),
            Err(err) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ApiErrorBody {
                        detail: err.to_string(),
                    }),
                ))
            }
        }
    }
    body.tasks = normalized_tasks;
    if let Err(err) = save_config(&state.config_path, &body).await {
        return Err(internal_error(err));
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn execute_now(State(state): State<AppState>, Json(body): Json<TaskItem>) -> ApiResult<OrderResult> {
    let task = normalize_task(&body).map_err(bad_request)?;
    let result = run_task(&state, &task).await;
    Ok(Json(result))
}

async fn resolve_quantity(
    State(state): State<AppState>,
    Json(body): Json<QuantityResolveRequest>,
) -> ApiResult<ResolveQuantityResponse> {
    let task = TaskItem {
        name: String::new(),
        exchange: body.exchange,
        symbol: body.symbol,
        quantity_mode: body.quantity_mode,
        quantity: body.quantity,
        side: "long".to_string(),
        order_type: "market".to_string(),
        price: body.price,
        leverage: body.leverage,
        scheduled_at: String::new(),
        enabled: true,
        cron: None,
    };
    let normalized = normalize_task(&task).map_err(bad_request)?;
    let payload = resolve_order_quantity_for_task(&state, &normalized, "margin")
        .await
        .map_err(bad_gateway)?;
    Ok(Json(payload))
}

async fn list_exchanges() -> ApiResult<Vec<String>> {
    Ok(Json(
        SUPPORTED_EXCHANGES
            .iter()
            .map(|item| item.to_string())
            .collect(),
    ))
}

async fn list_contracts(
    State(state): State<AppState>,
    Query(query): Query<ContractsQuery>,
) -> ApiResult<ContractPayload> {
    let exchange = normalize_exchange(Some(&query.exchange), true).map_err(bad_request)?;
    let ttl = Duration::from_secs(300);
    if !query.refresh {
        let cache = state.contract_cache.lock().await;
        if let Some(entry) = cache.get(&exchange) {
            if entry.cached_at.elapsed() < ttl {
                return Ok(Json(ContractPayload {
                    exchange,
                    symbols: entry.symbols.clone(),
                    count: entry.symbols.len(),
                    cached: true,
                    stale: false,
                    updated_at: entry.updated_at.format("%Y-%m-%dT%H:%M:%S").to_string(),
                    message: None,
                }));
            }
        }
    }
    let client = state
        .clients
        .get(&exchange)
        .ok_or_else(|| bad_request(anyhow::anyhow!("Unsupported exchange: {}", exchange)))?;
    match client.list_contract_symbols().await {
        Ok(symbols) => {
            let updated_at = Local::now();
            {
                let mut cache = state.contract_cache.lock().await;
                cache.insert(
                    exchange.clone(),
                    CachedSymbols {
                        symbols: symbols.clone(),
                        updated_at,
                        cached_at: Instant::now(),
                    },
                );
            }
            Ok(Json(ContractPayload {
                exchange,
                count: symbols.len(),
                symbols,
                cached: false,
                stale: false,
                updated_at: updated_at.format("%Y-%m-%dT%H:%M:%S").to_string(),
                message: None,
            }))
        }
        Err(err) => {
            error!("failed to list contract symbols: {}", err);
            let cache = state.contract_cache.lock().await;
            if let Some(entry) = cache.get(&exchange) {
                Ok(Json(ContractPayload {
                    exchange,
                    symbols: entry.symbols.clone(),
                    count: entry.symbols.len(),
                    cached: true,
                    stale: true,
                    updated_at: entry.updated_at.format("%Y-%m-%dT%H:%M:%S").to_string(),
                    message: Some(err.to_string()),
                }))
            } else {
                Err(bad_gateway(err))
            }
        }
    }
}

async fn get_balance(Query(query): Query<BalanceQuery>) -> ApiResult<serde_json::Value> {
    let exchange = normalize_exchange(Some(&query.exchange), true).map_err(bad_request)?;
    Err((
        StatusCode::BAD_REQUEST,
        Json(ApiErrorBody {
            detail: format!("{} does not support balance queries in Rust version yet.", exchange),
        }),
    ))
}

async fn get_order_status(Query(query): Query<OrderStatusQuery>) -> ApiResult<serde_json::Value> {
    let exchange = normalize_exchange(Some(&query.exchange), true).map_err(bad_request)?;
    let symbol_hint = query.symbol.unwrap_or_default();
    Err((
        StatusCode::BAD_REQUEST,
        Json(ApiErrorBody {
            detail: format!(
                "{} does not support order status queries in Rust version yet (order_id={}, symbol={}).",
                exchange, query.order_id, symbol_hint
            ),
        }),
    ))
}

fn bad_request(err: anyhow::Error) -> (StatusCode, Json<ApiErrorBody>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ApiErrorBody {
            detail: err.to_string(),
        }),
    )
}

fn bad_gateway(err: anyhow::Error) -> (StatusCode, Json<ApiErrorBody>) {
    (
        StatusCode::BAD_GATEWAY,
        Json(ApiErrorBody {
            detail: err.to_string(),
        }),
    )
}

fn internal_error(err: anyhow::Error) -> (StatusCode, Json<ApiErrorBody>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ApiErrorBody {
            detail: err.to_string(),
        }),
    )
}

