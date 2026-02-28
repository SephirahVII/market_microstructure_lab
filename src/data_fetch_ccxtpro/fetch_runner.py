# src/data_fetch_ccxtpro/fetch_runner.py

import sys
import os
import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timezone

# === 1. 环境路径设置 ===
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(src_dir)

sys.path.append(project_root)

from src.utils import ensure_dir, load_config
from src.data_fetch_ccxtpro.trade import TradeCollector
from src.data_fetch_ccxtpro.orderbook import OrderbookCollector

# Configure Logging
log_dir = os.path.join(project_root, 'logs')
ensure_dir(log_dir)
log_file = os.path.join(log_dir, 'fetcher.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('fetch_runner')

async def main():
    # === 2. 加载配置 ===
    config_path = os.path.join(project_root, 'config', 'fetch_config.yaml')
    config = load_config(config_path)
    if not config: return

    # === 3. 确定数据输出路径 ===
    raw_subdir = config['system'].get('raw_data_subdir', 'raw')
    data_root = os.path.join(project_root, 'data', raw_subdir)
    proxy_url = config['system'].get('proxy_url')

    now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"[{now_utc} UTC] 🚀 Starting Data Collection System")
    logger.info(f"[{now_utc} UTC] 📂 Data storage path: {data_root}")
    logger.info(f"[{now_utc} UTC] 🌐 Proxy setting: {proxy_url if proxy_url else 'None'}")
    logger.info("-" * 50)

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
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        logger.warning(f"[{now_utc} UTC] ⚠️ No collection tasks enabled, please check config/collector_config.yaml")
        return

    # === 5. 并行执行 ===
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"[{now_utc} UTC] 🛑 Task gathering cancelled, initiating closure sequence")
    except Exception as e:
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        logger.exception(f"[{now_utc} UTC] 💥 System level error: {e}")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    main_task = loop.create_task(main())
    try:
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"[{now_utc} UTC] 🛑 KeyboardInterrupt! Initiating graceful shutdown...")
        main_task.cancel()
        # Give 5 seconds for tasks to process CancelledError and run finally blocks (exchange.close)
        try:
            loop.run_until_complete(asyncio.wait_for(main_task, timeout=5.0))
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
    finally:
        # Cancel any stubborn/background tasks to avoid unclosed session complaints
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]
        for task in tasks:
            task.cancel()
        
        loop.run_until_complete(asyncio.sleep(0.5))
        try:
            loop.close()
        except Exception:
            pass
