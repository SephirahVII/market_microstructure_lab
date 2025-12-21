# src/collectors/trade.py
import ccxt.pro
import asyncio
import csv
import os
from src.utils import ensure_dir, get_safe_symbol

class TradeCollector:
    def __init__(self, data_root, proxy_url=None):
        self.output_dir = os.path.join(data_root, 'trades')
        self.proxy_url = proxy_url
        self.csv_headers = [
            'timestamp', 'datetime', 'symbol', 'side', 
            'price', 'amount', 'cost', 'trade_id', 'type'
        ]
        ensure_dir(self.output_dir)

    def _get_file_path(self, exchange_id, market_type, symbol, trade_datetime):
        if not trade_datetime: 
            return None
            
        date_str = trade_datetime.split('T')[0]
        safe_symbol = get_safe_symbol(symbol)
        
        # === 📂 路径修改：增加 market_type 层级 ===
        directory = os.path.join(self.output_dir, market_type, exchange_id, safe_symbol)
        ensure_dir(directory)
        return os.path.join(directory, f"{date_str}.csv")

    def save_trade(self, exchange_id, market_type, trade):
        symbol = trade['symbol']
        raw_datetime = trade.get('datetime')
        
        # 传入 market_type 获取路径
        file_path = self._get_file_path(exchange_id, market_type, symbol, raw_datetime)
        if not file_path: return

        # 格式化时间 (去掉 T/Z 以匹配 Orderbook 格式)
        csv_datetime = raw_datetime
        if raw_datetime:
            csv_datetime = raw_datetime.replace('T', ' ').replace('Z', '')

        file_exists = os.path.isfile(file_path)
        
        try:
            with open(file_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(self.csv_headers)
                
                row = [
                    trade.get('timestamp'), 
                    csv_datetime,
                    trade.get('symbol'),
                    trade.get('side'), 
                    trade.get('price'), 
                    trade.get('amount'),
                    trade.get('cost'), 
                    trade.get('id'), 
                    trade.get('type')
                ]
                writer.writerow(row)
        except Exception as e:
            print(f"❌ [Trade][{exchange_id}] 写入错误: {e}")

    async def monitor_symbol(self, exchange, symbol, market_type):
        exchange_id = exchange.id
        print(f"🔹 [Trades] 启动: {exchange_id} ({market_type}) - {symbol}")
        last_id = None
        
        while True:
            try:
                trades = await exchange.watch_trades(symbol)
                for trade in trades:
                    if trade['id'] != last_id:
                        # 传递 market_type
                        self.save_trade(exchange_id, market_type, trade)
                        last_id = trade['id']
            except Exception as e:
                print(f"⚠️ [Trades][{exchange_id}] {symbol} 异常: {str(e)[:50]}")
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
        except Exception as e:
            print(f"💥 [Trades] {exchange_id} 初始化失败: {e}")
        finally:
            await exchange.close()

    async def run(self, exchange_configs):
        tasks = [self.run_exchange(conf) for conf in exchange_configs]
        await asyncio.gather(*tasks)
