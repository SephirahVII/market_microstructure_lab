import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Iterable, Optional

import pandas as pd
import yaml


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def get_safe_symbol(symbol: str) -> str:
    return symbol.replace('/', '_').replace(':', '_')


def load_config(config_path: str) -> Optional[dict]:
    if not os.path.isfile(config_path):
        print(f"❌ 配置文件不存在: {config_path}")
        return None
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def write_parquet_batch(
    output_dir: str,
    records: Iterable[dict],
    file_prefix: str,
    columns: Optional[list[str]] = None,
) -> str:
    ensure_dir(output_dir)
    file_name = f"{file_prefix}-{int(pd.Timestamp.utcnow().timestamp() * 1000)}.parquet"
    file_path = os.path.join(output_dir, file_name)
    df = pd.DataFrame(records)
    if columns:
        df = df.reindex(columns=columns)
    df.to_parquet(file_path, index=False)
    return file_path


def setup_logging(log_dir: str, log_name: str = "collector.log") -> logging.Logger:
    ensure_dir(log_dir)
    logger = logging.getLogger("market_microstructure")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = RotatingFileHandler(
        os.path.join(log_dir, log_name), maxBytes=5_000_000, backupCount=5
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
