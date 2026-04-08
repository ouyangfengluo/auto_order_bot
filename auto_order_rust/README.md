# auto_order_rust

Rust 版自动下单服务，当前支持：
- Binance
- Gate.io
- Bybit
- OKX

## 运行

```bash
cd auto_order_rust
cargo run
```

默认监听 `0.0.0.0:2888`，可通过环境变量覆盖：
- `AUTO_ORDER_HOST`
- `AUTO_ORDER_PORT`
- `AUTO_ORDER_CONFIG_PATH`，默认 `config.json`
- `AUTO_ORDER_STATIC_DIR`，默认 `static`

## 配置结构

`config.json` 现在包含两类任务：

```json
{
  "enabled": true,
  "tasks": [],
  "strategy_tasks": []
}
```

`tasks` 是普通定时下单任务。

`strategy_tasks` 是量化策略任务，当前内置：
- `minute_drop_short`

示例：

```json
{
  "id": "minute_drop_short:gateio:BTC_USDT",
  "name": "Gate BTC 1m 下跌做空",
  "strategy_kind": "minute_drop_short",
  "exchange": "gateio",
  "symbol": "BTC_USDT",
  "amount": 10,
  "leverage": 10,
  "enabled": true
}
```

## 策略说明

`minute_drop_short` 的行为：
- 初始化时读取近 3 根已收盘 1 分钟 K 线
- 找出涨幅最大的那根
- 按 `(最高价 - 最低价) / 2` 计算 `use_close_price`
- 后续每分钟收盘时，如果该分钟为下跌，则按 `min(下单金额, 合约账户余额)` 市价开空
- 开空后立即挂 `use_close_price` 的限价平仓单
- 若 10 分钟内仍未平仓，则直接市价平仓

当前完整运行能力优先支持 Gate.io 合约。

## API

- `GET /api/config`
- `POST /api/config`
- `GET /api/strategies`
- `POST /api/strategies`
- `POST /api/execute`
- `POST /api/strategies/execute`
- `POST /api/resolve-quantity`
- `GET /api/exchanges`
- `GET /api/contracts?exchange=binance`

## 已知限制

- Rust 版本的 `/api/balance` 与 `/api/order-status` 仍未对外提供通用查询接口
- 量化策略运行时状态保存在内存中，服务重启后会重新初始化策略参考价格与运行状态
