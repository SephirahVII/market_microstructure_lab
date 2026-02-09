# scripts/run_collector.py
import sys
import os
import asyncio

# === 1. 环境路径设置 ===
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

sys.path.append(project_root)

from src.utils import load_config, setup_logging
from src.collectors.trade import TradeCollector
from src.collectors.orderbook import OrderbookCollector
from src.realtime.app import create_app
from src.realtime.hub import BroadcastHub
from uvicorn import Config, Server

async def main():
    # === 2. 加载配置 ===
    config_path = os.path.join(project_root, 'config', 'collector_config.yaml')
    config = load_config(config_path)
    if not config: return

    # === 3. 确定数据输出路径 ===
    raw_subdir = config['system'].get('raw_data_subdir', 'raw')
    data_root = os.path.join(project_root, 'data', raw_subdir)
    proxy_url = config['system'].get('proxy_url')
    log_dir = os.path.join(project_root, config['system'].get('log_dir', 'logs'))
    logger = setup_logging(log_dir, console_level=30)
    dashboard_enabled = config['system'].get('dashboard_enabled', False)
    dashboard_port = config['system'].get('dashboard_port', 8000)

    logger.info("🚀 启动数据采集系统")
    logger.info("📂 数据存放路径: %s", data_root)
    logger.info("🌐 代理设置: %s", proxy_url if proxy_url else 'None')
    logger.info("-" * 50)

    tasks = []
    server_task = None
    hub = None
    if dashboard_enabled:
        hub = BroadcastHub()
        app = create_app(hub)
        server = Server(
            Config(app=app, host="0.0.0.0", port=dashboard_port, log_level="warning")
        )
        server_task = asyncio.create_task(server.serve())

    # === 4. 初始化采集任务 ===
    # Trade data
    if config['trades'].get('enabled'):
        t_collector = TradeCollector(
            data_root, proxy_url, logger=logger, event_handler=hub.broadcast if hub else None
        )
        tasks.append(t_collector.run(config['trades']['exchanges']))

    # Orderbook data
    if config['orderbooks'].get('enabled'):
        ob_collector = OrderbookCollector(
            data_root, 
            depth_levels=config['orderbooks'].get('depth_levels', 20),
            proxy_url=proxy_url,
            logger=logger,
            event_handler=hub.broadcast if hub else None,
        )
        tasks.append(ob_collector.run(config['orderbooks']['exchanges']))

    if not tasks:
        logger.warning("⚠️ 未启用任何采集任务，请检查 config/collector_config.yaml")
        return

    # === 5. 并行执行 ===
    if server_task:
        tasks.append(server_task)

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("🛑 用户停止程序")
    except Exception as e:
        logger.exception("💥 系统级错误: %s", e)
    finally:
        if server_task:
            server_task.cancel()
            await asyncio.gather(server_task, return_exceptions=True)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
