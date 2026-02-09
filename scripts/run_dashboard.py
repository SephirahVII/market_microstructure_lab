import asyncio
import os
import sys

from uvicorn import Config, Server

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from src.collectors.orderbook import OrderbookCollector
from src.collectors.trade import TradeCollector
from src.realtime.app import create_app
from src.realtime.hub import BroadcastHub
from src.utils import load_config, setup_logging


async def main() -> None:
    config_path = os.path.join(project_root, 'config', 'collector_config.yaml')
    config = load_config(config_path)
    if not config:
        return

    raw_subdir = config['system'].get('raw_data_subdir', 'raw_parquet')
    data_root = os.path.join(project_root, 'data', raw_subdir)
    proxy_url = config['system'].get('proxy_url')
    log_dir = os.path.join(project_root, config['system'].get('log_dir', 'logs'))
    logger = setup_logging(log_dir, log_name="dashboard.log")

    hub = BroadcastHub()
    app = create_app(hub)

    logger.info("🚀 启动采集 + Dashboard")
    logger.info("📂 数据存放路径: %s", data_root)
    logger.info("🌐 代理设置: %s", proxy_url if proxy_url else 'None')

    collectors = []
    if config['trades'].get('enabled'):
        t_collector = TradeCollector(
            data_root, proxy_url, logger=logger, event_handler=hub.broadcast
        )
        collectors.append(asyncio.create_task(t_collector.run(config['trades']['exchanges'])))

    if config['orderbooks'].get('enabled'):
        ob_collector = OrderbookCollector(
            data_root,
            depth_levels=config['orderbooks'].get('depth_levels', 20),
            proxy_url=proxy_url,
            logger=logger,
            event_handler=hub.broadcast,
        )
        collectors.append(asyncio.create_task(ob_collector.run(config['orderbooks']['exchanges'])))

    if not collectors:
        logger.warning("⚠️ 未启用任何采集任务，请检查 config/collector_config.yaml")
        return

    server = Server(
        Config(app=app, host="0.0.0.0", port=8000, log_level="info", lifespan="on")
    )
    server_task = asyncio.create_task(server.serve())

    try:
        await asyncio.gather(server_task, *collectors)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("🛑 用户停止程序")
    except Exception as e:
        logger.exception("💥 系统级错误: %s", e)
    finally:
        for task in collectors:
            task.cancel()
        server_task.cancel()
        await asyncio.gather(server_task, *collectors, return_exceptions=True)


if __name__ == '__main__':
    asyncio.run(main())
