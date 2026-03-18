# 自动下单机器人

合约定时下单工具，支持 Binance、Gate.io、Bybit。项目使用 FastAPI 提供后端接口和前端配置页面，启动后可直接在浏览器中配置任务。

## 功能

- 配置具体下单时间，支持年月日时分秒选择
- 支持 Binance / Gate.io / Bybit
- 支持市价单和限价单
- 支持做多、做空
- 支持杠杆倍数设置
- 支持立即执行单次下单

## 快速开始

```bash
cd auto_order_bot
pip install -r requirements.txt
python main.py
```

浏览器访问 `http://localhost:8000` 进入配置页面。

## 环境变量

在项目根目录创建 `.env` 文件：

```env
# Binance 合约
BINANCE_API_KEY=your_key
BINANCE_API_SECRET=your_secret

# Gate.io 合约
GATEIO_API_KEY=your_key
GATEIO_API_SECRET=your_secret

# Bybit 合约
BYBIT_API_KEY=your_key
BYBIT_API_SECRET=your_secret
```

按需配置你要使用的交易所即可。

## 项目结构

```text
auto_order_bot/
├── main.py            # FastAPI 入口
├── config.json        # 任务配置（运行后自动生成）
├── requirements.txt
├── sdks/              # 各交易所合约 SDK
│   ├── base_contract_sdk.py
│   ├── binance_contract_sdk.py
│   ├── gateio_contract_sdk.py
│   └── bybit_contract_sdk.py
└── static/
    └── index.html     # 配置页面
```

## 时间说明

- 下单时间使用具体日期时间，不再使用 cron 表达式
- 前端输入框支持日历和时间选择器
- 新建任务默认填充当前本地时间
- 定时任务执行完成后会自动禁用，避免重复触发
