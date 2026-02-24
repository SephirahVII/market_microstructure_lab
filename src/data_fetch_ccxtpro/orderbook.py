# src/data_fetch_ccxtpro/orderbook.py
import ccxt.pro
import asyncio
import os
import time
import pandas as pd
import logging
from datetime import datetime, timezone
from utils import ensure_dir, get_safe_symbol

logger = logging.getLogger('OrderbookCollector')

class OrderbookCollector:
    def __init__(self, data_root, depth_levels=20, proxy_url=None, buffer_size=1000, flush_interval=10):
        self.data_root = data_root
        self.output_dir = os.path.join(data_root, 'orderbooks')
        self.depth_levels = depth_levels
        self.proxy_url = proxy_url
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.last_hour = None
        
        self.buffer = []
        self.buffer_lock = asyncio.Lock()
        self.write_lock = asyncio.Lock()
        
        # Start a background task to flush data periodically
        self.flush_task = asyncio.create_task(self._periodic_flush())

    async def _periodic_flush(self):
        while True:
            await asyncio.sleep(self.flush_interval)
            await self.flush()

    async def flush(self):
        async with self.buffer_lock:
            if not self.buffer:
                return
            data_to_flush = self.buffer[:]
            self.buffer.clear()
            
        async with self.write_lock:
            try:
                # CPU bound task, offload to a background thread
                await asyncio.to_thread(self._write_parquet, data_to_flush)
            except Exception as e:
                now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                logger.error(f"[{now_utc} UTC] ❌ [Orderbook] Parquet batch write error: {e}", exc_info=True)

    def _write_parquet(self, data):
        if not data:
            return
            
        df = pd.DataFrame(data)
        
        # Group by partition keys
        partition_keys = ['market_type', 'exchange', 'symbol', 'date', 'hour']
        for name, group in df.groupby(partition_keys):
            market_type, exchange, symbol, date_str, hour_str = name
            
            if self.last_hour is None:
                self.last_hour = hour_str
            elif self.last_hour != hour_str:
                now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"[{now_utc} UTC] 🕒 [Orderbook] File split rolled over from {self.last_hour} to {hour_str}. Previous Parquet files successfully saved.")
                self.last_hour = hour_str
            
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
            
            # Write to Parquet (read and concat if exists to avoid fastparquet append bugs)
            if os.path.exists(file_path):
                try:
                    existing_df = pd.read_parquet(file_path)
                    save_df = pd.concat([existing_df, save_df], ignore_index=True)
                except Exception as e:
                    now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    corrupted_path = f"{file_path}.corrupted_{int(time.time())}"
                    try:
                        os.rename(file_path, corrupted_path)
                        logger.error(f"[{now_utc} UTC] 🛑 [Orderbook] 严重文件损坏! 无法读取历史 Parquet 尾部: {e}")
                        logger.error(f"[{now_utc} UTC] 🛡️ [Orderbook] 为了防止历史数据被覆盖，现已安全隔离为: {corrupted_path}")
                    except Exception as re_err:
                        logger.error(f"[{now_utc} UTC] ❌ [Orderbook] 重命名损坏文件失败: {re_err}")
                    
            save_df.to_parquet(file_path, engine='pyarrow', compression='snappy')

    async def save_snapshot(self, exchange_id, market_type, symbol, orderbook, local_ts):
        exchange_ts = orderbook.get('timestamp', local_ts)
        if not exchange_ts: 
            exchange_ts = local_ts
            
        dt_obj = datetime.utcfromtimestamp(local_ts / 1000)
        date_str = dt_obj.strftime('%Y-%m-%d')
        # 改为按分钟切片 %H%M，如 1700, 1701...
        hour_str = dt_obj.strftime('%H%M')
        
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

        async with self.buffer_lock:
            self.buffer.append(row)
            should_flush = len(self.buffer) >= self.buffer_size

        if should_flush:
            await self.flush()

    async def monitor_symbol(self, exchange, symbol, market_type):
        """
        全量监控模式 (无 Interval 限制)
        """
        exchange_id = exchange.id
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"[{now_utc} UTC] 🔹 [Orderbook] Started monitoring: {exchange_id} ({market_type}) - {symbol}")
        
        while True:
            try:
                # 🛡️ 增加超时保护，防止无限等待导致堵塞
                orderbook = await asyncio.wait_for(exchange.watch_order_book(symbol), timeout=30.0)
                
                # 获取当前时间 (本地接收时间, 毫秒)
                local_ts = int(time.time() * 1000)
                
                await self.save_snapshot(exchange_id, market_type, symbol, orderbook, local_ts)

            except asyncio.TimeoutError:
                now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                logger.warning(f"[{now_utc} UTC] ⚠️ [Orderbook][{exchange_id}] {symbol} Data timeout (30s no push), reconnecting...")
                try:
                    await exchange.close()
                except:
                    pass
                
            except Exception as e:
                now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                logger.error(f"[{now_utc} UTC] ⚠️ [Orderbook][{exchange_id}] {symbol} Exception: {str(e)[:100]}", exc_info=True)
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
            now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            logger.error(f"[{now_utc} UTC] 💥 [Orderbook] {exchange_id} Initialization failed: {e}", exc_info=True)
        finally:
            await exchange.close()

    async def run(self, exchange_configs):
        try:
            tasks = [self.run_exchange(conf) for conf in exchange_configs]
            await asyncio.gather(*tasks)
        finally:
            # Ensure buffer is flushed when shutting down
            await self.flush()
