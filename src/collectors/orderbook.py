# src/collectors/orderbook.py
import ccxt.pro
import asyncio
import csv
import os
import time
from datetime import datetime
from src.utils import ensure_dir, get_safe_symbol

class OrderbookCollector:
    def __init__(self, data_root, depth_levels=20, proxy_url=None):
        self.output_dir = os.path.join(data_root, 'orderbooks')
        self.depth_levels = depth_levels
        self.proxy_url = proxy_url
        self.csv_headers = self._generate_headers()
        ensure_dir(self.output_dir)

    def _generate_headers(self):
        headers = ['local_ts', 'exchange_ts', 'datetime', 'symbol']
        for i in range(self.depth_levels):
            headers.extend([f'bid_p_{i+1}', f'bid_q_{i+1}'])
        for i in range(self.depth_levels):
            headers.extend([f'ask_p_{i+1}', f'ask_q_{i+1}'])
        return headers

    def _get_file_path(self, exchange_id, market_type, symbol):
        # 强制使用 UTC 日期作为文件名
        date_str = datetime.utcnow().strftime('%Y-%m-%d')
        safe_symbol = get_safe_symbol(symbol)
        
        # === 📂 路径增加 market_type ===
        directory = os.path.join(self.output_dir, market_type, exchange_id, safe_symbol)
        ensure_dir(directory)
        return os.path.join(directory, f"{date_str}.csv")

    def save_snapshot(self, exchange_id, market_type, symbol, orderbook, local_ts):
        exchange_ts = orderbook.get('timestamp', local_ts)
        if not exchange_ts: exchange_ts = local_ts
            
        dt_obj = datetime.utcfromtimestamp(local_ts / 1000)
        dt_str = dt_obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        # 传递 market_type
        file_path = self._get_file_path(exchange_id, market_type, symbol)
        file_exists = os.path.isfile(file_path)

        try:
            with open(file_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(self.csv_headers)
                
                row = [local_ts, exchange_ts, dt_str, symbol]
                for side in ['bids', 'asks']:
                    items = orderbook.get(side, [])
                    for i in range(self.depth_levels):
                        if i < len(items):
                            row.extend([items[i][0], items[i][1]])
                        else:
                            row.extend(['', ''])
                writer.writerow(row)
        except Exception as e:
            print(f"❌ [Orderbook][{exchange_id}] 写入错误: {e}")

    async def monitor_symbol(self, exchange, symbol, market_type):
        """
        全量监控模式 (无 Interval 限制)
        """
        exchange_id = exchange.id
        print(f"🔹 [Orderbook] 启动: {exchange_id} ({market_type}) - {symbol}")
        
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
                self.save_snapshot(exchange_id, market_type, symbol, orderbook, local_ts)

            except asyncio.TimeoutError:
                # 捕获超时，打印警告并让循环继续，触发重连
                print(f"⚠️ [Orderbook][{exchange_id}] {symbol} 数据超时 (30s 无推送)，正在重连...")
                # 可以在这里尝试显式关闭连接，帮助清理状态
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
        except Exception as e:
            print(f"💥 [Orderbook] {exchange_id} 初始化失败: {e}")
        finally:
            await exchange.close()

    async def run(self, exchange_configs):
        tasks = [self.run_exchange(conf) for conf in exchange_configs]
        await asyncio.gather(*tasks)
