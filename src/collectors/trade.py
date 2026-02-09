# src/collectors/trade.py
import asyncio
import logging
import os
import time
from typing import Awaitable, Callable, Optional

import ccxt.pro

from src.utils import ensure_dir, get_safe_symbol, write_parquet_batch

class TradeCollector:
    def __init__(
        self,
        data_root,
        proxy_url=None,
        logger: Optional[logging.Logger] = None,
        event_handler: Optional[Callable[[dict], Awaitable[None]]] = None,
        batch_size: int = 200,
        flush_interval: int = 5,
    ):
        self.output_dir = os.path.join(data_root, 'trades')
        self.proxy_url = proxy_url
        self.parquet_columns = [
            'timestamp', 'datetime', 'symbol', 'side', 
            'price', 'amount', 'cost', 'trade_id', 'type'
        ]
        self.event_handler = event_handler
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._buffers: dict[str, list[dict]] = {}
        self._last_flush: dict[str, float] = {}
        self.logger = logger or logging.getLogger(__name__)
        ensure_dir(self.output_dir)

    def _get_output_dir(self, exchange_id, market_type, symbol, trade_datetime):
        if not trade_datetime: 
            return None
            
        date_str = trade_datetime.split('T')[0]
        safe_symbol = get_safe_symbol(symbol)
        
        # === 📂 路径修改：增加 market_type 层级 ===
        directory = os.path.join(self.output_dir, market_type, exchange_id, safe_symbol, date_str)
        ensure_dir(directory)
        return directory

    def _append_record(self, output_dir: str, record: dict) -> None:
        buffer = self._buffers.setdefault(output_dir, [])
        buffer.append(record)
        last_flush = self._last_flush.get(output_dir, 0)
        if len(buffer) >= self.batch_size or time.time() - last_flush >= self.flush_interval:
            self._flush(output_dir)

    def _flush(self, output_dir: str) -> None:
        records = self._buffers.get(output_dir, [])
        if not records:
            return
        payload = list(records)
        self._buffers[output_dir] = []
        try:
            file_path = write_parquet_batch(
                output_dir, payload, "trades", columns=self.parquet_columns
            )
            self._last_flush[output_dir] = time.time()
            self.logger.info("✅ [Trade] 写入 Parquet: %s", file_path)
        except Exception as exc:
            self.logger.exception("❌ [Trade] 写入 Parquet 失败: %s", exc)

    def flush_all(self) -> None:
        for output_dir in list(self._buffers.keys()):
            self._flush(output_dir)

    async def _emit_event(self, payload: dict) -> None:
        if not self.event_handler:
            return
        result = self.event_handler(payload)
        if asyncio.iscoroutine(result):
            await result

    def save_trade(self, exchange_id, market_type, trade):
        symbol = trade['symbol']
        raw_datetime = trade.get('datetime')
        
        # 传入 market_type 获取路径
        output_dir = self._get_output_dir(exchange_id, market_type, symbol, raw_datetime)
        if not output_dir:
            return

        # 格式化时间 (去掉 T/Z 以匹配 Orderbook 格式)
        csv_datetime = raw_datetime
        if raw_datetime:
            csv_datetime = raw_datetime.replace('T', ' ').replace('Z', '')
        record = {
            'timestamp': trade.get('timestamp'),
            'datetime': csv_datetime,
            'symbol': trade.get('symbol'),
            'side': trade.get('side'),
            'price': trade.get('price'),
            'amount': trade.get('amount'),
            'cost': trade.get('cost'),
            'trade_id': trade.get('id'),
            'type': trade.get('type'),
        }
        self._append_record(output_dir, record)
        return record

    async def monitor_symbol(self, exchange, symbol, market_type):
        exchange_id = exchange.id
        self.logger.info("🔹 [Trades] 启动: %s (%s) - %s", exchange_id, market_type, symbol)
        last_id = None
        
        while True:
            try:
                trades = await exchange.watch_trades(symbol)
                for trade in trades:
                    if trade['id'] != last_id:
                        # 传递 market_type
                        record = self.save_trade(exchange_id, market_type, trade)
                        if record:
                            await self._emit_event(
                                {
                                    "type": "trade",
                                    "exchange_id": exchange_id,
                                    "market_type": market_type,
                                    "symbol": symbol,
                                    "payload": record,
                                }
                            )
                        last_id = trade['id']
            except Exception as e:
                self.logger.warning(
                    "⚠️ [Trades][%s] %s 异常: %s", exchange_id, symbol, str(e)[:200]
                )
                await asyncio.sleep(5)

    async def run_exchange(self, config_item):
        exchange_id = config_item['exchange']
        symbols = config_item['symbols']
        # 默认为 'spot'
        market_type = config_item.get('market_type', 'spot')
        
        options = {
            'enableRateLimit': True,
            'defaultType': market_type
        }
        
        if self.proxy_url:
            options['proxies'] = {'http': self.proxy_url, 'https': self.proxy_url}

        exchange_class = getattr(ccxt.pro, exchange_id)
        exchange = exchange_class(options)
        
        try:
            # 将 market_type 传给监控任务
            await asyncio.gather(*[self.monitor_symbol(exchange, s, market_type) for s in symbols])
        except asyncio.CancelledError:
            self.logger.info("🛑 [Trades] %s 收到取消信号，准备关闭连接", exchange_id)
            raise
        except Exception as e:
            self.logger.exception("💥 [Trades] %s 初始化失败: %s", exchange_id, e)
        finally:
            self.flush_all()
            await exchange.close()

    async def run(self, exchange_configs):
        tasks = [self.run_exchange(conf) for conf in exchange_configs]
        await asyncio.gather(*tasks)
