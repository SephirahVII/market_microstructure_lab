# src/collectors/orderbook.py
import asyncio
import logging
import os
import time
from datetime import datetime
from typing import Awaitable, Callable, Optional

import ccxt.pro

from src.utils import ensure_dir, get_safe_symbol, write_parquet_batch

class OrderbookCollector:
    def __init__(
        self,
        data_root,
        depth_levels=20,
        proxy_url=None,
        logger: Optional[logging.Logger] = None,
        event_handler: Optional[Callable[[dict], Awaitable[None]]] = None,
        batch_size: int = 50,
        flush_interval: int = 5,
    ):
        self.output_dir = os.path.join(data_root, 'orderbooks')
        self.depth_levels = depth_levels
        self.proxy_url = proxy_url
        self.parquet_columns = self._generate_headers()
        self.event_handler = event_handler
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._buffers: dict[str, list[dict]] = {}
        self._last_flush: dict[str, float] = {}
        self.logger = logger or logging.getLogger(__name__)
        ensure_dir(self.output_dir)

    def _generate_headers(self):
        headers = ['local_ts', 'exchange_ts', 'datetime', 'symbol']
        for i in range(self.depth_levels):
            headers.extend([f'bid_p_{i+1}', f'bid_q_{i+1}'])
        for i in range(self.depth_levels):
            headers.extend([f'ask_p_{i+1}', f'ask_q_{i+1}'])
        return headers

    def _get_output_dir(self, exchange_id, market_type, symbol):
        # 强制使用 UTC 日期作为文件名
        date_str = datetime.utcnow().strftime('%Y-%m-%d')
        safe_symbol = get_safe_symbol(symbol)
        
        # === 📂 路径增加 market_type ===
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
                output_dir, payload, "orderbook", columns=self.parquet_columns
            )
            self._last_flush[output_dir] = time.time()
            self.logger.info("✅ [Orderbook] 写入 Parquet: %s", file_path)
        except Exception as exc:
            self.logger.exception("❌ [Orderbook] 写入 Parquet 失败: %s", exc)

    def flush_all(self) -> None:
        for output_dir in list(self._buffers.keys()):
            self._flush(output_dir)

    async def _emit_event(self, payload: dict) -> None:
        if not self.event_handler:
            return
        result = self.event_handler(payload)
        if asyncio.iscoroutine(result):
            await result

    def save_snapshot(self, exchange_id, market_type, symbol, orderbook, local_ts):
        exchange_ts = orderbook.get('timestamp', local_ts)
        if not exchange_ts: exchange_ts = local_ts
            
        dt_obj = datetime.utcfromtimestamp(local_ts / 1000)
        dt_str = dt_obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        # 传递 market_type
        output_dir = self._get_output_dir(exchange_id, market_type, symbol)

        record = {
            'local_ts': local_ts,
            'exchange_ts': exchange_ts,
            'datetime': dt_str,
            'symbol': symbol,
        }
        for side in ['bids', 'asks']:
            items = orderbook.get(side, [])
            for i in range(self.depth_levels):
                price_key = f'{side[:-1]}_p_{i+1}'
                qty_key = f'{side[:-1]}_q_{i+1}'
                if i < len(items):
                    record[price_key] = items[i][0]
                    record[qty_key] = items[i][1]
                else:
                    record[price_key] = None
                    record[qty_key] = None

        self._append_record(output_dir, record)
        return record

    async def monitor_symbol(self, exchange, symbol, market_type):
        """
        全量监控模式 (无 Interval 限制)
        """
        exchange_id = exchange.id
        self.logger.info("🔹 [Orderbook] 启动: %s (%s) - %s", exchange_id, market_type, symbol)
        
        while True:
            try:
                # === 🛡️ 修改点：增加超时保护 ===
                # 如果 10 秒内（或者设为 30 秒）没有收到数据，强制抛出超时异常
                # 这样可以防止 ccxt 内部崩溃导致程序无限等待
                orderbook = await asyncio.wait_for(exchange.watch_order_book(symbol), timeout=30.0)
                
                # 2. 获取当前时间 (本地接收时间)
                now = time.time()
                local_ts = int(now * 1000)
                
                # 3. 直接写入
                record = self.save_snapshot(exchange_id, market_type, symbol, orderbook, local_ts)
                if record:
                    await self._emit_event(
                        {
                            "type": "orderbook",
                            "exchange_id": exchange_id,
                            "market_type": market_type,
                            "symbol": symbol,
                            "payload": {
                                "symbol": symbol,
                                "local_ts": local_ts,
                                "bids": orderbook.get("bids", [])[: self.depth_levels],
                                "asks": orderbook.get("asks", [])[: self.depth_levels],
                            },
                        }
                    )

            except asyncio.TimeoutError:
                # 捕获超时，打印警告并让循环继续，触发重连
                self.logger.warning(
                    "⚠️ [Orderbook][%s] %s 数据超时 (30s 无推送)，正在重连...",
                    exchange_id,
                    symbol,
                )
                # 可以在这里尝试显式关闭连接，帮助清理状态
                try:
                    await exchange.close()
                except:
                    pass
                
            except Exception as e:
                self.logger.warning(
                    "⚠️ [Orderbook][%s] %s 异常: %s", exchange_id, symbol, str(e)[:200]
                )
                await asyncio.sleep(5)

    async def run_exchange(self, config_item):
        exchange_id = config_item['exchange']
        symbols = config_item['symbols']
        # 提取 market_type，默认为 spot
        market_type = config_item.get('market_type', 'spot')

        options = {
            'enableRateLimit': True, 
            'newUpdates': False,
            'defaultType': market_type # 设置 ccxt 连接类型
        }
        
        if self.proxy_url:
            options['proxies'] = {'http': self.proxy_url, 'https': self.proxy_url}

        exchange_class = getattr(ccxt.pro, exchange_id)
        exchange = exchange_class(options)
        
        try:
            # 修正：将 market_type 传递给 monitor_symbol
            await asyncio.gather(*[self.monitor_symbol(exchange, s, market_type) for s in symbols])
        except asyncio.CancelledError:
            self.logger.info("🛑 [Orderbook] %s 收到取消信号，准备关闭连接", exchange_id)
            raise
        except Exception as e:
            self.logger.exception("💥 [Orderbook] %s 初始化失败: %s", exchange_id, e)
        finally:
            self.flush_all()
            await exchange.close()

    async def run(self, exchange_configs):
        tasks = [self.run_exchange(conf) for conf in exchange_configs]
        await asyncio.gather(*tasks)
