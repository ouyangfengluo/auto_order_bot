"""
半箱体套利交易脚本 - 重构版
使用SDK模式封装各交易所功能
"""
import os
import time
import json
import requests
import logging
from dotenv import load_dotenv
import sys
from typing import Optional, Tuple, Dict, Any

# 导入SDK工厂
from sdks import ExchangeSDKFactory
from notice import send_feishu_message, send_opsalert_phone_notification

# 单腿时用 Lighter 合约对冲（可选）
try:
    import lighter_hedge as lighter_hedge
except ImportError:
    lighter_hedge = None

load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('half_box_arbitrage.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('HalfBoxArbitrage')

# 配置参数
API_BASE = os.getenv("OPTIONS_API_BASE", "http://localhost:3000/api")
POLL_INTERVAL = float(os.getenv("HALFBOX_POLL_INTERVAL", "1"))
MIN_PROFIT_PCT = float(os.getenv("HALFBOX_MIN_PROFIT_PCT", "0"))

# 排除交易所列表
EXCLUDED_EXCHANGES = []
if EXCLUDED_EXCHANGES:
    logger.info(f"已排除交易所: {', '.join(EXCLUDED_EXCHANGES)}")

def send_trade_notification(title: str, content: str, phone: bool = False) -> None:
    """
    交易通知统一出口：默认飞书；当 phone=True 时同时触发电话/语音通知。
    """
    try:
        send_feishu_message(title, content)
    except Exception as e:
        logger.warning(f"飞书通知异常: {e}")

    if phone:
        try:
            send_opsalert_phone_notification(title=title, content=content, priority=1)
        except Exception as e:
            logger.warning(f"电话通知异常: {e}")

# ==================== OpsAlert MsgPush（电话通知）配置 ====================
# 参考 python/【余额监控+电话通知】.py 的 send_opsalert_notification
OPSALERT_MSG_PUSH_URL = 'https://www.opsalert.cn/msgpush/api/send/814dd28c-fefd-47b3-8bc9-feacdebc64ef'
OPSALERT_NOTIFY_COOLDOWN_SECONDS = int(os.getenv("OPSALERT_NOTIFY_COOLDOWN_SECONDS", str(24 * 60 * 60)))
OPSALERT_LAST_NOTIFY_FILE = os.getenv("OPSALERT_LAST_NOTIFY_FILE", "opsalert_last_notify.json")


def _load_last_opsalert_ts() -> Optional[int]:
    try:
        if not os.path.exists(OPSALERT_LAST_NOTIFY_FILE):
            return None
        with open(OPSALERT_LAST_NOTIFY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        ts = data.get("last_notify_ts")
        return int(ts) if ts is not None else None
    except Exception as e:
        logger.warning(f"读取OpsAlert限频文件失败，忽略限频: {e}")
        return None


def _save_last_opsalert_ts(ts: int) -> None:
    try:
        with open(OPSALERT_LAST_NOTIFY_FILE, "w", encoding="utf-8") as f:
            json.dump({"last_notify_ts": int(ts)}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"写入OpsAlert限频文件失败: {e}")


def _opsalert_can_notify_now(now_ts: Optional[int] = None) -> bool:
    if not OPSALERT_MSG_PUSH_URL:
        return False
    now = int(now_ts or time.time())
    last = _load_last_opsalert_ts()
    if last is None:
        return True
    return (now - last) >= OPSALERT_NOTIFY_COOLDOWN_SECONDS


def send_opsalert_notification(title: str, content: str, priority: int = 1) -> bool:
    """
    发送 OpsAlert 电话/语音通知（通过 MsgPush）。
    - 仅在配置了 OPSALERT_MSG_PUSH_URL 且未触发24小时限频时发送
    """
    if not OPSALERT_MSG_PUSH_URL:
        return False
    if not _opsalert_can_notify_now():
        logger.info("OpsAlert：24小时限频中，跳过电话通知")
        return False

    payload = {
        "title": title,
        "priority": priority,
        "content": content,
    }

    try:
        resp = requests.post(
            OPSALERT_MSG_PUSH_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code in (200, 201):
            _save_last_opsalert_ts(int(time.time()))
            logger.info(f"OpsAlert推送成功 | 状态码: {resp.status_code} | 响应: {resp.text}")
            return True
        logger.warning(f"OpsAlert推送失败 | 状态码: {resp.status_code} | 响应: {resp.text}")
        return False
    except Exception as e:
        logger.warning(f"OpsAlert请求异常: {e}")
        return False


def _extract_available(balance: Optional[dict]) -> float:
    if not balance or not isinstance(balance, dict):
        return 0.0
    available = float(balance.get("available", 0) or 0)
    if available > 0:
        return available
    return float(balance.get("total", 0) or 0)


def _detect_insufficient_balance_reason(
    refreshed: dict,
    call_balance: Optional[dict],
    put_balance: Optional[dict],
) -> Tuple[bool, str]:
    """
    判断 trade_quantity<=0 是否由余额不足引起，并给出可读原因。
    返回: (是否余额不足, 原因文本)
    """
    call_ex = refreshed.get("callExchange")
    put_ex = refreshed.get("putExchange")
    symbol = refreshed.get("symbol")
    call_price = float(refreshed.get("callPrice", 0) or 0)
    put_price = float(refreshed.get("putPrice", 0) or 0)

    # 用最大最小订单量作为“最小可交易数量”的粗略估计（两边都能满足才行）
    call_min = get_min_order_size(call_ex, symbol)
    put_min = get_min_order_size(put_ex, symbol)
    min_qty = max(float(call_min or 0), float(put_min or 0))
    if min_qty <= 0:
        min_qty = 0.01

    call_avail = _extract_available(call_balance)
    put_avail = _extract_available(put_balance)

    # 同交易所：需要余额覆盖组合成本
    if call_ex == put_ex:
        combo_price = call_price + put_price
        required = combo_price * min_qty
        if combo_price > 0 and call_avail < required:
            return True, f"同一交易所 {call_ex} 可用余额不足：可用 {call_avail:.4f} < 最小需求 {required:.4f}（最小数量 {min_qty}）"
        return False, "同一交易所余额检查通过或价格异常"

    # 不同交易所：分别检查两腿最小需求
    reasons = []
    if call_price > 0:
        required_call = call_price * min_qty
        if call_avail < required_call:
            reasons.append(f"Call腿 {call_ex} 可用 {call_avail:.4f} < 需求 {required_call:.4f}")
    if put_price > 0:
        required_put = put_price * min_qty
        if put_avail < required_put:
            reasons.append(f"Put腿 {put_ex} 可用 {put_avail:.4f} < 需求 {required_put:.4f}")

    if reasons:
        return True, f"余额不足（最小数量 {min_qty}）: " + "；".join(reasons)
    return False, "余额检查通过或价格异常"


def fetch_grouped_options():
    """获取分组的期权数据"""
    resp = requests.get(f"{API_BASE}/options", timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(f"options api failed: {data}")
    return data["data"] or {}


def find_half_box_opportunities(grouped_data):
    """
    查找半箱体套利机会
    
    Args:
        grouped_data: 分组的期权数据
        
    Returns:
        机会列表
    """
    opps = []
    
    for symbol, symbol_data in grouped_data.items():
        for expiry_time, ep_data in symbol_data.items():
            call_options = ep_data.get("CALL", {}) or {}
            put_options = ep_data.get("PUT", {}) or {}

            call_strikes = sorted(float(k) for k in call_options.keys())
            put_strikes = sorted(float(k) for k in put_options.keys())

            for k1 in call_strikes:
                for k2 in put_strikes:
                    if k1 >= k2:
                        continue

                    call_list = call_options.get(str(k1)) or []
                    put_list = put_options.get(str(k2)) or []
                    if not call_list or not put_list:
                        continue

                    # 过滤掉排除的交易所
                    call_list_filtered = [c for c in call_list if c.get("exchange") not in EXCLUDED_EXCHANGES]
                    put_list_filtered = [p for p in put_list if p.get("exchange") not in EXCLUDED_EXCHANGES]
                    
                    if not call_list_filtered or not put_list_filtered:
                        continue
                    
                    best_call = min(
                        (c for c in call_list_filtered if c.get("bestAsk", 0) > 0 and c.get("askVolume", 0) > 0),
                        key=lambda c: c["bestAsk"],
                        default=None,
                    )
                    best_put = min(
                        (p for p in put_list_filtered if p.get("bestAsk", 0) > 0 and p.get("askVolume", 0) > 0),
                        key=lambda p: p["bestAsk"],
                        default=None,
                    )
                    if not best_call or not best_put:
                        continue

                    call_price = best_call["bestAsk"]
                    put_price = best_put["bestAsk"]
                    total_cost = call_price + put_price
                    volume = min(best_call["askVolume"], best_put["askVolume"])

                    theoretical_value = k2 - k1
                    gross_profit_per_unit = theoretical_value - total_cost
                    profit_base = total_cost if total_cost > 0 else 1.0
                    profit_pct = (gross_profit_per_unit / profit_base) * 100.0
                    
                    if profit_pct < MIN_PROFIT_PCT:
                        continue

                    opps.append({
                        "symbol": symbol,
                        "expiryTime": int(expiry_time),
                        "strikePrice1": k1,
                        "strikePrice2": k2,
                        "callExchange": best_call["exchange"],
                        "putExchange": best_put["exchange"],
                        "callPrice": call_price,
                        "putPrice": put_price,
                        "totalCost": total_cost,
                        "theoreticalValue": theoretical_value,
                        "callVolume": best_call["askVolume"],
                        "putVolume": best_put["askVolume"],
                        "tradeVolume": volume,
                        "profitPerUnit": gross_profit_per_unit,
                        "profitPercentage": profit_pct,
                        "callOptionName": best_call.get("optionName"),
                        "putOptionName": best_put.get("optionName"),
                    })
    
    return opps


def pick_best_opportunity(opps):
    """选择最佳机会"""
    if not opps:
        return None
    filtered = [o for o in opps if o.get("profitPercentage", -999) >= MIN_PROFIT_PCT]
    if not filtered:
        return None
    return sorted(filtered, key=lambda o: o["profitPercentage"], reverse=True)[0]


def refresh_option_quote(exchange_name, option_name, symbol, expiry_time, strike_price, option_type):
    """
    通过SDK刷新期权报价
    
    Args:
        exchange_name: 交易所名称
        option_name: 期权名称
        symbol: 标的符号
        expiry_time: 到期时间
        strike_price: 行权价
        option_type: 期权类型
        
    Returns:
        {"bestAsk": float, "askVolume": float} 或 None
    """
    try:
        sdk = ExchangeSDKFactory.get_sdk(exchange_name)
        return sdk.get_ticker(option_name, symbol, expiry_time, strike_price, option_type)
    except Exception as e:
        logger.error(f"{exchange_name} 获取报价失败: {e}")
        return None


# 获取余额时使用的安全系数，仅使用可用余额的 95% 以防保证金不足
BALANCE_SAFETY_FACTOR = 0.95


def get_exchange_balance(exchange_name):
    """
    通过SDK获取交易所余额。
    为防止保证金不足，返回的 available/total 已乘以 BALANCE_SAFETY_FACTOR（默认 0.95）。
    
    Args:
        exchange_name: 交易所名称
        symbol: 标的符号
        
    Returns:
        {"available": float, "total": float} 或 None
    """
    try:
        sdk = ExchangeSDKFactory.get_sdk(exchange_name)
        raw = sdk.get_balance()
        if not raw or not isinstance(raw, dict):
            return raw
        available = float(raw.get("available", 0) or 0)
        total = float(raw.get("total", 0) or 0)
        return {
            "available": available * BALANCE_SAFETY_FACTOR,
            "total": total * BALANCE_SAFETY_FACTOR,
        }
    except Exception as e:
        logger.error(f"{exchange_name} 获取余额失败: {e}")
        return None


def get_min_order_size(exchange_name, symbol):
    """
    通过SDK获取最小订单数量
    
    Args:
        exchange_name: 交易所名称
        symbol: 标的符号
        
    Returns:
        最小订单数量
    """
    try:
        sdk = ExchangeSDKFactory.get_sdk(exchange_name)
        return sdk.get_coin_min_order_size(symbol)
    except Exception as e:
        logger.error(f"{exchange_name} 获取最小订单量失败: {e}")
        return 0.01  # 默认值


def adjust_quantity_for_min_order_size(quantity, min_order_size):
    """
    根据最小订单量调整数量
    
    Args:
        quantity: 原始数量
        min_order_size: 最小订单量
        
    Returns:
        调整后的数量
    """
    if min_order_size <= 0:
        return quantity
    
    # 确保数量不小于最小订单量
    if quantity < min_order_size:
        return 0  # 如果数量太小，返回0表示无法交易
    
    # 将数量调整为最小订单量的整数倍（向下取整）
    adjusted_quantity = int(quantity / min_order_size) * min_order_size
    return max(adjusted_quantity, 0)


def calculate_trade_quantity(refreshed_opp, call_balance=None, put_balance=None):
    """计算交易数量"""
    order_volume_limit = min(
        refreshed_opp.get("callVolume", 0),
        refreshed_opp.get("putVolume", 0)
    )
    
    call_exchange = refreshed_opp.get("callExchange")
    put_exchange = refreshed_opp.get("putExchange")
    call_price = refreshed_opp.get("callPrice", 0)
    put_price = refreshed_opp.get("putPrice", 0)
    symbol = refreshed_opp.get("symbol", "")
    
    # 获取两个交易所的最小订单量
    call_min_order_size = get_min_order_size(call_exchange, symbol)
    put_min_order_size = get_min_order_size(put_exchange, symbol)
    
    # 使用较大的最小订单量作为限制
    max_min_order_size = max(call_min_order_size, put_min_order_size)
    
    logger.info(f"Call交易所 {call_exchange} 最小订单量: {call_min_order_size}")
    logger.info(f"Put交易所 {put_exchange} 最小订单量: {put_min_order_size}")
    logger.info(f"使用最大最小订单量: {max_min_order_size}")
    
    # 如果是同一交易所，使用组合价格计算
    if call_exchange == put_exchange:
        logger.info(f"同一交易所 {call_exchange}，使用组合价格计算")
        combo_price = call_price + put_price
        if combo_price <= 0:
            return 0
        
        # 使用call_balance作为交易所余额
        available_balance = 0
        if call_balance and isinstance(call_balance, dict):
            available_balance = call_balance.get("available", 0)
            if available_balance <= 0:
                available_balance = call_balance.get("total", 0)
        
        balance_limit = available_balance / combo_price if combo_price > 0 else 0
        trade_qty = min(order_volume_limit, balance_limit)
        
        logger.info(f"余额限制: {available_balance:.4f} / {combo_price:.4f} = {balance_limit:.2f}")
        logger.info(f"订单量限制: {order_volume_limit:.2f}")
        logger.info(f"调整前数量: {trade_qty:.2f}")
        
        # 根据最小订单量调整
        adjusted_qty = adjust_quantity_for_min_order_size(trade_qty, max_min_order_size)
        logger.info(f"调整后数量: {adjusted_qty:.2f}")
        
        return max(0, adjusted_qty)
    
    # 不同交易所，分别计算
    call_balance_limit = float('inf')
    if call_balance and isinstance(call_balance, dict):
        call_balance_limit = call_balance.get("available", float('inf'))
        if call_balance_limit == float('inf'):
            call_balance_limit = call_balance.get("total", float('inf'))
    
    put_balance_limit = float('inf')
    if put_balance and isinstance(put_balance, dict):
        put_balance_limit = put_balance.get("available", float('inf'))
        if put_balance_limit == float('inf'):
            put_balance_limit = put_balance.get("total", float('inf'))
    
    # 根据价格计算余额能支持的数量
    if call_price > 0 and call_balance_limit != float('inf'):
        call_balance_limit = call_balance_limit / call_price
    
    if put_price > 0 and put_balance_limit != float('inf'):
        put_balance_limit = put_balance_limit / put_price
    
    trade_qty = min(
        order_volume_limit,
        call_balance_limit if call_balance_limit != float('inf') else order_volume_limit,
        put_balance_limit if put_balance_limit != float('inf') else order_volume_limit
    )
    
    logger.info(f"调整前数量: {trade_qty:.2f}")
    
    # 根据最小订单量调整
    adjusted_qty = adjust_quantity_for_min_order_size(trade_qty, max_min_order_size)
    logger.info(f"调整后数量: {adjusted_qty:.2f}")
    
    return max(0, adjusted_qty)


def place_order_with_retry(exchange_name, option_name, symbol, side, quantity, price, order_type="limit", max_retries=3):
    """
    通过SDK下单（带重试机制）
    
    Args:
        exchange_name: 交易所名称
        option_name: 期权名称
        symbol: 标的符号
        side: 买卖方向
        quantity: 数量
        price: 价格
        order_type: 订单类型
        max_retries: 最大重试次数
        
    Returns:
        {"success": bool, "order_id": str, "message": str, "retry_count": int}
    """
    for attempt in range(max_retries + 1):  # +1 因为第一次不算重试
        try:
            sdk = ExchangeSDKFactory.get_sdk(exchange_name)
            min_order_size = sdk.get_coin_min_order_size(symbol)
            result = sdk.place_order(option_name, symbol, side, quantity, price, order_type, min_order_size)
            
            if result and result.get("success"):
                if attempt > 0:
                    logger.info(f"{exchange_name} 第{attempt}次重试成功")
                result["retry_count"] = attempt
                return result
            else:
                error_msg = result.get("message", "未知错误") if result else "下单失败"
                if attempt < max_retries:
                    logger.warning(f"{exchange_name} 第{attempt + 1}次尝试失败: {error_msg}，准备重试...")
                    time.sleep(1)  # 等待1秒后重试
                else:
                    logger.error(f"{exchange_name} 所有重试均失败: {error_msg}")
                    return {"success": False, "message": error_msg, "retry_count": attempt}
                    
        except Exception as e:
            error_msg = str(e)
            if attempt < max_retries:
                logger.warning(f"{exchange_name} 第{attempt + 1}次尝试异常: {error_msg}，准备重试...")
                time.sleep(1)  # 等待1秒后重试
            else:
                logger.error(f"{exchange_name} 所有重试均异常: {error_msg}")
                return {"success": False, "message": error_msg, "retry_count": attempt}
    
    return {"success": False, "message": "未知错误", "retry_count": max_retries}


def query_order_status(exchange_name, order_id, option_name=None):
    """
    通过SDK查询订单状态
    
    Args:
        exchange_name: 交易所名称
        order_id: 订单ID
        option_name: 期权名称
        
    Returns:
        {"success": bool, "status": str, "filled_qty": float, "message": str}
    """
    try:
        sdk = ExchangeSDKFactory.get_sdk(exchange_name)
        return sdk.query_order_status(order_id, option_name)
    except Exception as e:
        logger.error(f"{exchange_name} 查询异常: {e}")
        return {"success": False, "message": str(e)}


def query_positions(exchange_name, symbol):
    """
    通过SDK查询持仓
    
    Args:
        exchange_name: 交易所名称
        symbol: 标的符号
        
    Returns:
        {"success": bool, "positions": list, "message": str}
    """
    try:
        sdk = ExchangeSDKFactory.get_sdk(exchange_name)
        return sdk.query_positions(symbol)
    except Exception as e:
        logger.error(f"{exchange_name} 查询异常: {e}")
        return {"success": False, "message": str(e)}





def analyze_hedge_strategies(failed_leg_info, successful_leg_info, grouped_data, symbol):
    """
    分析单腿对冲策略
    
    Args:
        failed_leg_info: 失败腿信息 {"type": "call/put", "strike": float, "exchange": str, "option_name": str}
        successful_leg_info: 成功腿信息 {"type": "call/put", "strike": float, "exchange": str, "option_name": str, "quantity": float}
        grouped_data: 分组的期权数据
        symbol: 标的符号
        
    Returns:
        对冲策略列表，按损失从小到大排序
    """
    strategies = []
    
    try:
        # 获取当前期权数据
        symbol_data = grouped_data.get(symbol, {})
        if not symbol_data:
            return [{"type": "无数据", "description": "无法获取期权数据进行对冲分析", "estimated_loss": "未知"}]
        
        successful_type = successful_leg_info["type"]
        successful_strike = successful_leg_info["strike"]
        successful_exchange = successful_leg_info["exchange"]
        successful_quantity = successful_leg_info["quantity"]
        
        failed_type = failed_leg_info["type"]
        failed_strike = failed_leg_info["strike"]
        
        # 策略1: 半箱体对冲 - 寻找其他行权价组合
        for expiry_time, ep_data in symbol_data.items():
            call_options = ep_data.get("CALL", {}) or {}
            put_options = ep_data.get("PUT", {}) or {}
            
            if successful_type == "call":
                # 成功腿是Call，需要找Put来组成半箱体
                for put_strike_str, put_list in put_options.items():
                    put_strike = float(put_strike_str)
                    if put_strike <= successful_strike:  # 确保是半箱体结构
                        continue
                    
                    best_put = min(
                        (p for p in put_list if p.get("bestAsk", 0) > 0 and p.get("askVolume", 0) >= successful_quantity),
                        key=lambda p: p["bestAsk"],
                        default=None,
                    )
                    
                    if best_put:
                        theoretical_value = put_strike - successful_strike
                        put_cost = best_put["bestAsk"] * successful_quantity
                        hedge_profit = theoretical_value * successful_quantity - put_cost
                        
                        strategies.append({
                            "type": "半箱体对冲",
                            "description": f"买入 {best_put['exchange']} Put {put_strike} @ {best_put['bestAsk']:.4f}",
                            "estimated_loss": -hedge_profit if hedge_profit < 0 else 0,
                            "details": f"理论价值: {theoretical_value:.4f}, 成本: {put_cost:.4f}",
                            "exchange": best_put['exchange'],
                            "option_name": best_put.get('optionName', ''),
                            "price": best_put["bestAsk"],
                            "quantity": successful_quantity
                        })
            
            elif successful_type == "put":
                # 成功腿是Put，需要找Call来组成半箱体
                for call_strike_str, call_list in call_options.items():
                    call_strike = float(call_strike_str)
                    if call_strike >= successful_strike:  # 确保是半箱体结构
                        continue
                    
                    best_call = min(
                        (c for c in call_list if c.get("bestAsk", 0) > 0 and c.get("askVolume", 0) >= successful_quantity),
                        key=lambda c: c["bestAsk"],
                        default=None,
                    )
                    
                    if best_call:
                        theoretical_value = successful_strike - call_strike
                        call_cost = best_call["bestAsk"] * successful_quantity
                        hedge_profit = theoretical_value * successful_quantity - call_cost
                        
                        strategies.append({
                            "type": "半箱体对冲",
                            "description": f"买入 {best_call['exchange']} Call {call_strike} @ {best_call['bestAsk']:.4f}",
                            "estimated_loss": -hedge_profit if hedge_profit < 0 else 0,
                            "details": f"理论价值: {theoretical_value:.4f}, 成本: {call_cost:.4f}",
                            "exchange": best_call['exchange'],
                            "option_name": best_call.get('optionName', ''),
                            "price": best_call["bestAsk"],
                            "quantity": successful_quantity
                        })
        
        # 策略2: 价差对冲 - 卖出相同期权
        for expiry_time, ep_data in symbol_data.items():
            option_type_data = ep_data.get(successful_type.upper(), {})
            strike_data = option_type_data.get(str(successful_strike), [])
            
            for option in strike_data:
                if option.get("bestBid", 0) > 0 and option.get("bidVolume", 0) >= successful_quantity:
                    sell_revenue = option["bestBid"] * successful_quantity
                    
                    strategies.append({
                        "type": "价差对冲",
                        "description": f"卖出 {option['exchange']} {successful_type.upper()} {successful_strike} @ {option['bestBid']:.4f}",
                        "estimated_loss": 0,  # 假设买卖价差为主要损失
                        "details": f"卖出收入: {sell_revenue:.4f}",
                        "exchange": option['exchange'],
                        "option_name": option.get('optionName', ''),
                        "price": option["bestBid"],
                        "quantity": successful_quantity
                    })
        
        # 策略3: 开合约对冲（简化分析）
        strategies.append({
            "type": "开合约对冲",
            "description": f"开{symbol}永续合约进行Delta对冲",
            "estimated_loss": "需要实时计算Delta和合约价格",
            "details": "通过永续合约对冲期权的Delta风险",
            "exchange": "多个交易所",
            "option_name": f"{symbol}-PERP",
            "price": "市价",
            "quantity": "根据Delta计算"
        })
        
    except Exception as e:
        strategies.append({
            "type": "分析错误",
            "description": f"对冲分析出错: {str(e)}",
            "estimated_loss": "未知"
        })
    
    # 按预估损失排序（数值损失优先，未知损失放后面）
    def sort_key(strategy):
        loss = strategy["estimated_loss"]
        if isinstance(loss, (int, float)):
            return (0, loss)  # 数值损失，按大小排序
        else:
            return (1, 0)  # 非数值损失，放在后面
    
    strategies.sort(key=sort_key)
    
    return strategies[:5]  # 返回前5个最优策略


def format_hedge_strategies(strategies):
    """
    格式化对冲策略为邮件内容
    
    Args:
        strategies: 对冲策略列表
        
    Returns:
        格式化的字符串
    """
    if not strategies:
        return "无可用对冲策略"
    
    result = "=== 推荐对冲策略 ===\n"
    
    for i, strategy in enumerate(strategies, 1):
        result += f"\n策略 {i}: {strategy['type']}\n"
        result += f"  操作: {strategy['description']}\n"
        
        if isinstance(strategy['estimated_loss'], (int, float)):
            if strategy['estimated_loss'] == 0:
                result += f"  预估损失: 无额外损失\n"
            else:
                result += f"  预估损失: {strategy['estimated_loss']:.4f}\n"
        else:
            result += f"  预估损失: {strategy['estimated_loss']}\n"
        
        if strategy.get('details'):
            result += f"  详情: {strategy['details']}\n"
    
    result += "\n⚠️ 以上分析仅供参考，实际执行前请确认市场情况和风险承受能力。"
    
    return result


def format_order_status(status_result):
    """格式化订单状态信息"""
    if not status_result or not status_result.get("success"):
        return "查询失败"
    
    status = status_result.get("status", "unknown")
    filled_qty = status_result.get("filled_qty", 0)
    
    status_map = {
        "0": "待成交",
        "1": "部分成交", 
        "2": "完全成交",
        "3": "已取消",
        "4": "已拒绝"
    }
    
    status_text = status_map.get(str(status), f"状态{status}")
    
    if filled_qty > 0:
        return f"{status_text} (成交量: {filled_qty})"
    else:
        return status_text


def main_loop():
    """主循环"""
    print(f"[half_box] 启动半箱体套利实盘交易（SDK重构版）")
    print(f"[half_box] API_BASE={API_BASE}, MIN_PROFIT_PCT={MIN_PROFIT_PCT}%")

    while True:
        try:
            # 步骤1: 获取期权数据并查找机会
            grouped = fetch_grouped_options()
            opps = find_half_box_opportunities(grouped)
            
            if not opps:
                time.sleep(POLL_INTERVAL)
                continue
            
            # 步骤2: 选出最佳机会
            best = pick_best_opportunity(opps)
            if not best:
                time.sleep(POLL_INTERVAL)
                continue
            
            print(f"\n[half_box] 找到最佳机会: {best['symbol']}, 收益率: {best['profitPercentage']:.2f}%")
            
            # 步骤3: 刷新报价
            refreshed = {
                "symbol": best["symbol"],
                "expiryTime": best["expiryTime"],
                "strikePrice1": best["strikePrice1"],
                "strikePrice2": best["strikePrice2"],
                "callExchange": best["callExchange"],
                "putExchange": best["putExchange"],
                "callOptionName": best.get("callOptionName"),
                "putOptionName": best.get("putOptionName"),
                "callPrice": best["callPrice"],
                "putPrice": best["putPrice"],
                "callVolume": best["callVolume"],
                "putVolume": best["putVolume"],
                "theoreticalValue": best["theoreticalValue"],
            }
            
            # 使用SDK刷新报价
            call_quote = refresh_option_quote(
                best["callExchange"],
                best.get("callOptionName", ""),
                best["symbol"],
                best["expiryTime"],
                best["strikePrice1"],
                "CALL"
            )
            if call_quote:
                refreshed["callPrice"] = call_quote["bestAsk"]
                refreshed["callVolume"] = call_quote["askVolume"]
                print(f"[refresh] Call({best['callExchange']}) 最新报价: {call_quote['bestAsk']:.4f}")
            
            put_quote = refresh_option_quote(
                best["putExchange"],
                best.get("putOptionName", ""),
                best["symbol"],
                best["expiryTime"],
                best["strikePrice2"],
                "PUT"
            )
            if put_quote:
                refreshed["putPrice"] = put_quote["bestAsk"]
                refreshed["putVolume"] = put_quote["askVolume"]
                print(f"[refresh] Put({best['putExchange']}) 最新报价: {put_quote['bestAsk']:.4f}")
            
            # 步骤4: 验证机会是否仍然存在
            refreshed_total_cost = refreshed["callPrice"] + refreshed["putPrice"]
            refreshed_profit_per_unit = refreshed["theoreticalValue"] - refreshed_total_cost
            refreshed_profit_pct = (refreshed_profit_per_unit / refreshed_total_cost * 100.0) if refreshed_total_cost > 0 else 0
            
            if refreshed_profit_pct < MIN_PROFIT_PCT:
                print(f"[half_box] 复核后机会消失 (收益率: {refreshed_profit_pct:.2f}%)")
                time.sleep(POLL_INTERVAL)
                continue
            
            print(f"[half_box] 复核后机会仍存在 (收益率: {refreshed_profit_pct:.2f}%)")
            
            # 步骤5: 获取余额
            call_balance = get_exchange_balance(best["callExchange"])
            put_balance = get_exchange_balance(best["putExchange"])
            
            # 如果是同一交易所，只需要获取一次余额
            if best["callExchange"] == best["putExchange"]:
                put_balance = call_balance
            
            # 步骤6: 计算交易数量
            trade_quantity = calculate_trade_quantity(refreshed, call_balance, put_balance)
            
            # 打印交易信息
            print("\n" + "="*60)
            print("【当前组合信息】")
            print(f"标的: {refreshed['symbol']}")
            print(f"Call交易所: {refreshed['callExchange']} - {refreshed.get('callOptionName', 'N/A')}")
            print(f"Put交易所: {refreshed['putExchange']} - {refreshed.get('putOptionName', 'N/A')}")
            print(f"收益率: {refreshed_profit_pct:.2f}%")
            print(f"应交易数量: {trade_quantity:.2f}")
            print("="*60 + "\n")
            
            if trade_quantity <= 0:
                print("[half_box] 交易数量为0，跳过")

                # 机会存在但数量为0：若是余额不足导致，则电话提醒（24小时仅一次）
                is_insufficient, reason = _detect_insufficient_balance_reason(refreshed, call_balance, put_balance)
                if is_insufficient:
                    title = f"半箱体套利机会但余额不足 - {refreshed.get('symbol', '')}"
                    content = (
                        f"检测到套利机会，但余额不足导致无法下单。\n\n"
                        f"标的: {refreshed.get('symbol', '')}\n"
                        f"Call交易所: {refreshed.get('callExchange', '')}\n"
                        f"Put交易所: {refreshed.get('putExchange', '')}\n"
                        f"Call价格: {float(refreshed.get('callPrice', 0) or 0):.6f}\n"
                        f"Put价格: {float(refreshed.get('putPrice', 0) or 0):.6f}\n"
                        f"复核收益率: {refreshed_profit_pct:.2f}%\n"
                        f"原因: {reason}\n"
                        f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    )
                    send_opsalert_notification(title=title, content=content, priority=1)

                time.sleep(POLL_INTERVAL)
                continue
            
            # 步骤7: 执行交易
            print(f"\n[half_box] 开始执行交易，数量: {trade_quantity:.2f}")
            
            # 下Call单（带重试）
            call_order_result = place_order_with_retry(
                exchange_name=refreshed['callExchange'],
                option_name=refreshed.get('callOptionName', ''),
                symbol=refreshed['symbol'],
                side="buy",
                quantity=trade_quantity,
                price=refreshed['callPrice']
            )
            
            if not call_order_result or not call_order_result.get("success"):
                error_msg = call_order_result.get("message", "未知错误") if call_order_result else "下单失败"
                retry_count = call_order_result.get("retry_count", 0) if call_order_result else 0
                
                print(f"[trade] ❌ Call期权下单失败（重试{retry_count}次后）: {error_msg}")
                
                # 发送失败通知（飞书 + 电话）
                failure_report = f"""
半箱体套利交易失败 ❌

=== 失败信息 ===
失败阶段: Call期权下单
交易所: {refreshed['callExchange']}
期权: {refreshed.get('callOptionName', 'N/A')}
重试次数: {retry_count}
失败原因: {error_msg}

=== 交易信息 ===
标的: {refreshed['symbol']}
预期收益率: {refreshed_profit_pct:.2f}%
计划交易数量: {trade_quantity:.2f}

=== 期权信息 ===
Call: {refreshed.get('callOptionName', 'N/A')} @ {refreshed['callPrice']:.4f}
Put: {refreshed.get('putOptionName', 'N/A')} @ {refreshed['putPrice']:.4f}

失败时间: {time.strftime('%Y-%m-%d %H:%M:%S')}

✅ 无单腿风险，可以重新寻找套利机会。
"""
                send_trade_notification(
                    title=f"半箱体套利交易失败 - {refreshed['symbol']}",
                    content=failure_report,
                    phone=True,
                )
                sys.exit(1)
            
            call_order_id = call_order_result.get("order_id")
            call_retry_count = call_order_result.get("retry_count", 0)
            if call_retry_count > 0:
                print(f"[trade] ✅ Call期权下单成功（重试{call_retry_count}次后），订单ID: {call_order_id}")
            else:
                print(f"[trade] ✅ Call期权下单成功，订单ID: {call_order_id}")
            
            # 下Put单（带重试）
            put_order_result = place_order_with_retry(
                exchange_name=refreshed['putExchange'],
                option_name=refreshed.get('putOptionName', ''),
                symbol=refreshed['symbol'],
                side="buy",
                quantity=trade_quantity,
                price=refreshed['putPrice']
            )
            
            if not put_order_result or not put_order_result.get("success"):
                error_msg = put_order_result.get("message", "未知错误") if put_order_result else "下单失败"
                retry_count = put_order_result.get("retry_count", 0) if put_order_result else 0
                symbol_put = refreshed['symbol']
                print(f"[trade] ❌ Put期权下单失败（重试{retry_count}次后）: {error_msg}")

                # 单腿：先尝试 Lighter 合约对冲（亏损<=1%），否则走期权最低风险方案 + 飞书+电话
                used_lighter_put = False
                if lighter_hedge and symbol_put:
                    can_lock, lock_msg, _ = lighter_hedge.check_lighter_hedge_loss_within_1pct(
                        symbol=symbol_put,
                        quantity=trade_quantity,
                        option_premium_per_unit=float(refreshed.get("callPrice") or 0),
                        is_call_leg=True,
                    )
                    print(f"[hedge] Lighter 合约对冲检查: {lock_msg}")
                    if can_lock:
                        ok, order_msg = lighter_hedge.place_lighter_perp_order_sync(
                            symbol=symbol_put, size=trade_quantity, is_long=False,
                        )
                        if ok:
                            used_lighter_put = True
                            print(f"[hedge] {order_msg}")
                            failure_report = f"""
半箱体套利 Put 下单失败，已用 Lighter 合约对冲 ❌→✅

=== 失败信息 ===
失败阶段: Put期权下单
失败原因: {error_msg}

=== 已成功 ===
Call期权订单ID: {call_order_id}，数量: {trade_quantity:.2f}
Lighter 合约对冲: {order_msg}
亏损已控制在≤1%以内，程序结束。

失败时间: {time.strftime('%Y-%m-%d %H:%M:%S')}
"""
                            send_trade_notification(
                                title=f"半箱体单腿 - 已Lighter对冲 - {symbol_put}",
                                content=failure_report,
                                phone=True,
                            )
                            sys.exit(1)

                if not used_lighter_put:
                    print(f"[hedge] 分析Call单腿对冲策略...")
                    failed_leg_info = {
                        "type": "put",
                        "strike": refreshed['strikePrice2'],
                        "exchange": refreshed['putExchange'],
                        "option_name": refreshed.get('putOptionName', '')
                    }
                    successful_leg_info = {
                        "type": "call",
                        "strike": refreshed['strikePrice1'],
                        "exchange": refreshed['callExchange'],
                        "option_name": refreshed.get('callOptionName', ''),
                        "quantity": trade_quantity
                    }
                    hedge_strategies = analyze_hedge_strategies(failed_leg_info, successful_leg_info, grouped, symbol_put)
                    hedge_analysis = format_hedge_strategies(hedge_strategies)
                    failure_report = f"""
半箱体套利交易失败 ❌

=== 失败信息 ===
失败阶段: Put期权下单
交易所: {refreshed['putExchange']}
期权: {refreshed.get('putOptionName', 'N/A')}
重试次数: {retry_count}
失败原因: {error_msg}

=== 已成功订单 ===
Call期权订单ID: {call_order_id} (已下单成功)
交易所: {refreshed['callExchange']}
期权: {refreshed.get('callOptionName', 'N/A')}
数量: {trade_quantity:.2f}

=== 交易信息 ===
标的: {symbol_put}
预期收益率: {refreshed_profit_pct:.2f}%
计划交易数量: {trade_quantity:.2f}

=== 期权信息 ===
Call: {refreshed.get('callOptionName', 'N/A')} @ {refreshed['callPrice']:.4f}
Put: {refreshed.get('putOptionName', 'N/A')} @ {refreshed['putPrice']:.4f}

⚠️ 无法用 Lighter 合约将亏损锁定至≤1%，请按以下最低风险方案处理：

{hedge_analysis}

失败时间: {time.strftime('%Y-%m-%d %H:%M:%S')}

⚠️ 注意: Call期权已成功下单，存在单腿风险，请尽快执行对冲策略！
"""
                    send_trade_notification(
                        title=f"半箱体套利交易失败 - {symbol_put}",
                        content=failure_report,
                        phone=True,
                    )
                sys.exit(1)
            
            put_order_id = put_order_result.get("order_id")
            put_retry_count = put_order_result.get("retry_count", 0)
            if put_retry_count > 0:
                print(f"[trade] ✅ Put期权下单成功（重试{put_retry_count}次后），订单ID: {put_order_id}")
            else:
                print(f"[trade] ✅ Put期权下单成功，订单ID: {put_order_id}")
            
            # 步骤8: 查询订单状态
            print(f"\n[trade] 等待订单成交...")
            time.sleep(3)
            
            call_status = query_order_status(refreshed['callExchange'], call_order_id, refreshed.get('callOptionName'))
            put_status = query_order_status(refreshed['putExchange'], put_order_id, refreshed.get('putOptionName'))
            
            print(f"[trade] Call订单状态: {format_order_status(call_status)}")
            print(f"[trade] Put订单状态: {format_order_status(put_status)}")
            
            # 检查订单是否成功
            call_success = call_status and call_status.get("success") and str(call_status.get("status")) == "2"
            put_success = put_status and put_status.get("success") and str(put_status.get("status")) == "2"
            
            trade_success = call_success and put_success
            
            # 步骤9: 查询持仓
            # call_positions = query_positions(refreshed['callExchange'], refreshed['symbol'])
            # put_positions = query_positions(refreshed['putExchange'], refreshed['symbol'])
            
            # 步骤10: 发送通知（飞书；失败/单腿则飞书+电话）
            call_status_text = format_order_status(call_status)
            put_status_text = format_order_status(put_status)
            
            success_indicator = "✅ 成功" if trade_success else "⚠️ 部分成功" if (call_success or put_success) else "❌ 失败"
            
            # 获取最小订单量信息用于报告
            call_min_order_size = get_min_order_size(refreshed['callExchange'], refreshed['symbol'])
            put_min_order_size = get_min_order_size(refreshed['putExchange'], refreshed['symbol'])
            max_min_order_size = max(call_min_order_size, put_min_order_size)
            
            # 获取重试信息
            call_retry_info = f" (重试{call_retry_count}次)" if call_retry_count > 0 else ""
            put_retry_info = f" (重试{put_retry_count}次)" if put_retry_count > 0 else ""
            
            report = f"""
半箱体套利交易报告 {success_indicator}

=== 交易信息 ===
标的: {refreshed['symbol']}
预期收益率: {refreshed_profit_pct:.2f}%
计算数量: {trade_quantity:.2f}
实际下单数量: {trade_quantity:.2f}

=== 最小订单量 ===
Call交易所 ({refreshed['callExchange']}): {call_min_order_size}
Put交易所 ({refreshed['putExchange']}): {put_min_order_size}
使用最大值: {max_min_order_size}

=== 订单详情 ===
Call期权 ({refreshed['callExchange']}){call_retry_info}:
  订单ID: {call_order_id}
  状态: {call_status_text}
  
Put期权 ({refreshed['putExchange']}){put_retry_info}:
  订单ID: {put_order_id}
  状态: {put_status_text}

=== 期权信息 ===
Call: {refreshed.get('callOptionName', 'N/A')} @ {refreshed['callPrice']:.4f}
Put: {refreshed.get('putOptionName', 'N/A')} @ {refreshed['putPrice']:.4f}
理论价值: {refreshed['theoreticalValue']:.4f}
总成本: {refreshed_total_cost:.4f}

交易时间: {time.strftime('%Y-%m-%d %H:%M:%S')}
"""
            
            notify_title = f"半箱体套利 {success_indicator} - {refreshed['symbol']}"
            send_trade_notification(
                title=notify_title,
                content=report,
                phone=not trade_success,
            )
            
            # 根据交易成功状态决定程序流程
            if trade_success:
                print("\n[half_box] ✅ 交易完全成功，继续监控新机会...")
                # 继续循环，不退出程序
            else:
                print(f"\n[half_box] ⚠️ 交易未完全成功 (Call: {call_success}, Put: {put_success})")
                symbol = refreshed["symbol"]
                # 单腿：先查 Lighter 合约是否可锁定风险（亏损<=1%），能则执行对冲并结束；否则走期权最低风险方案 + 飞书+电话
                used_lighter = False
                if lighter_hedge and symbol:
                    call_single = call_success and not put_success
                    put_single = put_success and not call_success
                    if call_single or put_single:
                        option_premium = float(refreshed.get("callPrice") or 0) if call_single else float(refreshed.get("putPrice") or 0)
                        can_lock, lock_msg, _ = lighter_hedge.check_lighter_hedge_loss_within_1pct(
                            symbol=symbol,
                            quantity=trade_quantity,
                            option_premium_per_unit=option_premium,
                            is_call_leg=call_single,
                        )
                        print(f"[hedge] Lighter 合约对冲检查: {lock_msg}")
                        if can_lock:
                            # Call单腿 → 开空；Put单腿 → 开多
                            is_long = put_single
                            ok, order_msg = lighter_hedge.place_lighter_perp_order_sync(
                                symbol=symbol,
                                size=trade_quantity,
                                is_long=is_long,
                            )
                            if ok:
                                used_lighter = True
                                print(f"[hedge] {order_msg}")
                                report += f"\n\n=== Lighter 合约对冲已执行 ===\n{order_msg}\n亏损已控制在≤1%以内，程序结束。\n"
                                send_trade_notification(
                                    title=f"半箱体单腿 - 已Lighter对冲 - {symbol}",
                                    content=report,
                                    phone=True,
                                )
                                print(f"[half_box] 程序退出（已执行 Lighter 对冲）")
                                sys.exit(1)
                            else:
                                print(f"[hedge] Lighter 下单未成功: {order_msg}，将走期权最低风险方案并通知")

                if not used_lighter:
                    # 无法用 Lighter 锁定至≤1% 或未启用：分析期权对冲方案，飞书+电话通知并结束
                    hedge_analysis = ""
                    if call_success and not put_success:
                        print(f"[hedge] 检测到Call单腿，分析对冲策略...")
                        failed_leg_info = {
                            "type": "put",
                            "strike": refreshed['strikePrice2'],
                            "exchange": refreshed['putExchange'],
                            "option_name": refreshed.get('putOptionName', '')
                        }
                        successful_leg_info = {
                            "type": "call",
                            "strike": refreshed['strikePrice1'],
                            "exchange": refreshed['callExchange'],
                            "option_name": refreshed.get('callOptionName', ''),
                            "quantity": trade_quantity
                        }
                        hedge_strategies = analyze_hedge_strategies(failed_leg_info, successful_leg_info, grouped, symbol)
                        hedge_analysis = format_hedge_strategies(hedge_strategies)
                    elif put_success and not call_success:
                        print(f"[hedge] 检测到Put单腿，分析对冲策略...")
                        failed_leg_info = {
                            "type": "call",
                            "strike": refreshed['strikePrice1'],
                            "exchange": refreshed['callExchange'],
                            "option_name": refreshed.get('callOptionName', '')
                        }
                        successful_leg_info = {
                            "type": "put",
                            "strike": refreshed['strikePrice2'],
                            "exchange": refreshed['putExchange'],
                            "option_name": refreshed.get('putOptionName', ''),
                            "quantity": trade_quantity
                        }
                        hedge_strategies = analyze_hedge_strategies(failed_leg_info, successful_leg_info, grouped, symbol)
                        hedge_analysis = format_hedge_strategies(hedge_strategies)

                    if hedge_analysis:
                        report += "\n\n⚠️ 无法用 Lighter 合约将亏损锁定至≤1%，请按以下最低风险方案处理：\n\n"
                        report += f"{hedge_analysis}\n"
                        report += "\n⚠️ 检测到单腿风险，请尽快执行推荐的对冲策略！"
                    else:
                        report += "\n\n⚠️ 单腿风险：无法用合约锁定至≤1%，且暂无期权对冲方案，请人工处理。"

                    send_trade_notification(
                        title=f"半箱体套利 {success_indicator} - {symbol}",
                        content=report,
                        phone=True,
                    )
                    print(f"[half_box] 程序退出")
                    sys.exit(1)
            
        except Exception as e:
            print(f"[half_box] 检测出错: {e}")
            import traceback
            traceback.print_exc()

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main_loop()