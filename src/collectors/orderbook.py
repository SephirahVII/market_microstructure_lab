# src/collectors/orderbook.py
import ccxt.pro
import asyncio
import os
import time
import pandas as pd
from datetime import datetime
from src.utils import ensure_dir, get_safe_symbol

class OrderbookCollector:
    def __init__(self, data_root, depth_levels=20, proxy_url=None, buffer_size=1000, flush_interval=10):
        self.data_root = data_root
        self.output_dir = os.path.join(data_root, 'orderbooks')
        self.depth_levels = depth_levels
        self.proxy_url = proxy_url
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        
        self.buffer = []
        self.lock = asyncio.Lock()
        
        # Start a background task to flush data periodically
        self.flush_task = asyncio.create_task(self._periodic_flush())

    async def _periodic_flush(self):
        while True:
            await asyncio.sleep(self.flush_interval)
            await self.flush()

    async def flush(self):
        async with self.lock:
            if not self.buffer:
                return
            data_to_flush = self.buffer[:]
            self.buffer.clear()
            
        try:
            # CPU bound task, offload to a background thread
            await asyncio.to_thread(self._write_parquet, data_to_flush)
        except Exception as e:
            print(f"❌ [Orderbook] Parquet 批量写入错误: {e}")

    def _write_parquet(self, data):
        if not data:
            return
            
        df = pd.DataFrame(data)
        
        # Group by partition keys
        partition_keys = ['market_type', 'exchange', 'symbol', 'date', 'hour']
        for name, group in df.groupby(partition_keys):
            market_type, exchange, symbol, date_str, hour_str = name
            
            # Construct Hive-style partition directory path
            dir_path = os.path.join(
                self.output_dir, 
                f"market_type={market_type}",
                f"exchange={exchange}",
                f"symbol={symbol}",
                f"date={date_str}"
            )
            ensure_dir(dir_path)
            
            file_path = os.path.join(dir_path, f"{hour_str}.parquet")
            
            # Remove partition columns to save storage space
            save_df = group.drop(columns=partition_keys)
            
            # Write to Parquet (append if exists)
            if os.path.exists(file_path):
                # fastparquet supports appending
                save_df.to_parquet(file_path, engine='fastparquet', append=True)
            else:
                save_df.to_parquet(file_path, engine='fastparquet', compression='snappy')

    async def save_snapshot(self, exchange_id, market_type, symbol, orderbook, local_ts):
        exchange_ts = orderbook.get('timestamp', local_ts)
        if not exchange_ts: 
            exchange_ts = local_ts
            
        dt_obj = datetime.utcfromtimestamp(local_ts / 1000)
        date_str = dt_obj.strftime('%Y-%m-%d')
        hour_str = dt_obj.strftime('%H')
        
        safe_symbol = get_safe_symbol(symbol)
        
        row = {
            'market_type': market_type,
            'exchange': exchange_id,
            'symbol': safe_symbol,
            'date': date_str,
            'hour': hour_str,
            'local_ts': local_ts,
            'exchange_ts': exchange_ts
        }
        
        # Flatten orderbook
        for side, side_prefix in [('bids', 'bid'), ('asks', 'ask')]:
            items = orderbook.get(side, [])
            for i in range(self.depth_levels):
                if i < len(items):
                    row[f'{side_prefix}_p_{i+1}'] = float(items[i][0])
                    row[f'{side_prefix}_q_{i+1}'] = float(items[i][1])
                else:
                    row[f'{side_prefix}_p_{i+1}'] = 0.0
                    row[f'{side_prefix}_q_{i+1}'] = 0.0

        async with self.lock:
            self.buffer.append(row)
            should_flush = len(self.buffer) >= self.buffer_size

        if should_flush:
            await self.flush()

    async def monitor_symbol(self, exchange, symbol, market_type):
        """
        全量监控模式 (无 Interval 限制)
        """
        exchange_id = exchange.id
        print(f"🔹 [Orderbook] 启动: {exchange_id} ({market_type}) - {symbol}")
        
        while True:
            try:
                # 🛡️ 增加超时保护，防止无限等待导致堵塞
                orderbook = await asyncio.wait_for(exchange.watch_order_book(symbol), timeout=30.0)
                
                # 获取当前时间 (本地接收时间, 毫秒)
                local_ts = int(time.time() * 1000)
                
                await self.save_snapshot(exchange_id, market_type, symbol, orderbook, local_ts)

            except asyncio.TimeoutError:
                print(f"⚠️ [Orderbook][{exchange_id}] {symbol} 数据超时 (30s 无推送)，正在重连...")
                try:
                    await exchange.close()
                except:
                    pass
                
            except Exception as e:
                print(f"⚠️ [Orderbook][{exchange_id}] {symbol} 异常: {str(e)[:100]}")
                await asyncio.sleep(5)

    async def run_exchange(self, config_item):
        exchange_id = config_item['exchange']
        symbols = config_item['symbols']
        market_type = config_item.get('market_type', 'spot')

        options = {
            'enableRateLimit': True, 
            'newUpdates': False,
            'defaultType': market_type
        }
        
        if self.proxy_url:
            options['proxies'] = {'http': self.proxy_url, 'https': self.proxy_url}

        exchange_class = getattr(ccxt.pro, exchange_id)
        exchange = exchange_class(options)
        
        try:
            await asyncio.gather(*[self.monitor_symbol(exchange, s, market_type) for s in symbols])
        except Exception as e:
            print(f"💥 [Orderbook] {exchange_id} 初始化失败: {e}")
        finally:
            await exchange.close()

    async def run(self, exchange_configs):
        try:
            tasks = [self.run_exchange(conf) for conf in exchange_configs]
            await asyncio.gather(*tasks)
        finally:
            # Ensure buffer is flushed when shutting down
            await self.flush()
