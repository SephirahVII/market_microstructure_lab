# src/data_fetch_ccxtpro/trade.py
import ccxt.pro
import asyncio
import os
import time
import pandas as pd
import logging
from datetime import datetime, timezone
from utils import ensure_dir, get_safe_symbol

logger = logging.getLogger('TradeCollector')

class TradeCollector:
    def __init__(self, data_root, proxy_url=None, buffer_size=5000, flush_interval=10):
        self.data_root = data_root
        self.output_dir = os.path.join(data_root, 'trades')
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
                # CPU bound task, offload to a background thread to prevent blocking the asyncio event loop
                await asyncio.to_thread(self._write_parquet, data_to_flush)
            except Exception as e:
                now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                logger.error(f"[{now_utc} UTC] ❌ [Trade] Parquet batch write error: {e}", exc_info=True)

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
                logger.info(f"[{now_utc} UTC] 🕒 [Trade] File split rolled over from {self.last_hour} to {hour_str}. Previous Parquet files successfully saved.")
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
            
            # Remove partition columns from the DataFrame to save storage space
            save_df = group.drop(columns=partition_keys).copy()
            if 'trade_id' in save_df.columns:
                save_df['trade_id'] = save_df['trade_id'].astype(object)
            
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
                        logger.error(f"[{now_utc} UTC] 🛑 [Trade] 严重文件损坏! 无法读取历史 Parquet 尾部: {e}")
                        logger.error(f"[{now_utc} UTC] 🛡️ [Trade] 为了防止历史数据被覆盖，现已安全隔离为: {corrupted_path}")
                    except Exception as re_err:
                        logger.error(f"[{now_utc} UTC] ❌ [Trade] 重命名损坏文件失败: {re_err}")
                    
            save_df.to_parquet(file_path, engine='pyarrow', compression='snappy')

    async def save_trade(self, exchange_id, market_type, trade):
        symbol = trade.get('symbol')
        raw_datetime = trade.get('datetime')
        if not raw_datetime or not symbol: 
            return

        # Extract Date and Hour from UTC datetime "2024-05-01T12:34:56.789Z"
        try:
            dt_obj = datetime.strptime(raw_datetime.split('.')[0].replace('Z', ''), "%Y-%m-%dT%H:%M:%S")
        except:
            dt_obj = datetime.now(timezone.utc)
            
        date_str = dt_obj.strftime('%Y-%m-%d')
        # 改为按分钟切片 %H%M，如 1700, 1701...
        hour_str = dt_obj.strftime('%H%M')
        
        safe_symbol = get_safe_symbol(symbol)
        
        # side format mapping: buy=1, sell=-1
        side_val = 1 if trade.get('side') == 'buy' else -1 if trade.get('side') == 'sell' else 0

        row = {
            'market_type': market_type,
            'exchange': exchange_id,
            'symbol': safe_symbol,
            'date': date_str,
            'hour': hour_str,
            'exchange_ts': trade.get('timestamp', int(time.time() * 1000)),
            'side': side_val,
            'price': float(trade.get('price', 0)) if trade.get('price') is not None else 0.0,
            'amount': float(trade.get('amount', 0)) if trade.get('amount') is not None else 0.0,
            'trade_id': str(trade.get('id', ''))
        }

        async with self.buffer_lock:
            self.buffer.append(row)
            should_flush = len(self.buffer) >= self.buffer_size

        if should_flush:
            await self.flush()

    async def monitor_symbol(self, exchange, symbol, market_type):
        exchange_id = exchange.id
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"[{now_utc} UTC] 🔹 [Trades] Started monitoring: {exchange_id} ({market_type}) - {symbol}")
        last_id = None
        
        while True:
            try:
                trades = await exchange.watch_trades(symbol)
                for trade in trades:
                    if trade['id'] != last_id:
                        await self.save_trade(exchange_id, market_type, trade)
                        last_id = trade['id']
            except Exception as e:
                now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                logger.warning(f"[{now_utc} UTC] ⚠️ [Trades][{exchange_id}] {symbol} Exception: {str(e)[:100]}")
                await asyncio.sleep(5)

    async def run_exchange(self, config_item):
        exchange_id = config_item['exchange']
        symbols = config_item['symbols']
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
            await asyncio.gather(*[self.monitor_symbol(exchange, s, market_type) for s in symbols])
        except Exception as e:
            now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            logger.error(f"[{now_utc} UTC] 💥 [Trades] {exchange_id} Initialization failed: {e}", exc_info=True)
        finally:
            await exchange.close()

    async def run(self, exchange_configs):
        try:
            tasks = [self.run_exchange(conf) for conf in exchange_configs]
            await asyncio.gather(*tasks)
        finally:
            # Ensure buffer is flushed when shutting down
            await self.flush()
