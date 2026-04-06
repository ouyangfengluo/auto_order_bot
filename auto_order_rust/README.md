# auto_order_rust

Rust 版本自动下单服务，迁移自 Python 版，保留以下交易所：

- Binance
- Gate.io
- Bybit
- OKX

`lighter` 相关能力已全部剔除。

## 运行

```bash
cd auto_order_rust
cargo run
```

默认监听 `0.0.0.0:2888`，可通过环境变量覆盖：

- `AUTO_ORDER_HOST`
- `AUTO_ORDER_PORT`
- `AUTO_ORDER_CONFIG_PATH`（默认 `config.json`）
- `AUTO_ORDER_STATIC_DIR`（默认 `static`）

## 主要接口

- `GET /api/config`
- `POST /api/config`
- `POST /api/execute`
- `POST /api/resolve-quantity`
- `GET /api/exchanges`
- `GET /api/contracts?exchange=binance`

## 已知差异

- 当前 Rust 版本未实现 `/api/balance` 与 `/api/order-status` 的交易所查询逻辑，接口会返回 `400`。
