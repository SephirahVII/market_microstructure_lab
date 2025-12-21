# scripts/run_collector.py
import sys
import os
import asyncio

# === 1. 环境路径设置 ===
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

sys.path.append(project_root)

from src.utils import load_config
from src.collectors.trade import TradeCollector
from src.collectors.orderbook import OrderbookCollector

async def main():
    # === 2. 加载配置 ===
    config_path = os.path.join(project_root, 'config', 'collector_config.yaml')
    config = load_config(config_path)
    if not config: return

    # === 3. 确定数据输出路径 ===
    raw_subdir = config['system'].get('raw_data_subdir', 'raw')
    data_root = os.path.join(project_root, 'data', raw_subdir)
    proxy_url = config['system'].get('proxy_url')

    print(f"🚀 启动数据采集系统")
    print(f"📂 数据存放路径: {data_root}")
    print(f"🌐 代理设置: {proxy_url if proxy_url else 'None'}")
    print("-" * 50)

    tasks = []

    # === 4. 初始化采集任务 ===
    # Trade data
    if config['trades'].get('enabled'):
        t_collector = TradeCollector(data_root, proxy_url)
        tasks.append(t_collector.run(config['trades']['exchanges']))

    # Orderbook data
    if config['orderbooks'].get('enabled'):
        ob_collector = OrderbookCollector(
            data_root, 
            depth_levels=config['orderbooks'].get('depth_levels', 20),
            proxy_url=proxy_url
        )
        tasks.append(ob_collector.run(config['orderbooks']['exchanges']))

    if not tasks:
        print("⚠️ 未启用任何采集任务，请检查 config/collector_config.yaml")
        return

    # === 5. 并行执行 ===
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\n🛑 用户停止程序")
    except Exception as e:
        print(f"\n💥 系统级错误: {e}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
