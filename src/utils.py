import os
import yaml

def ensure_dir(directory):
    """Ensure that the given directory exists."""
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

def get_safe_symbol(symbol):
    """Convert symbol to a safe format for filenames/directories."""
    # Examples: 'BTC/USDT' -> 'BTC_USDT', 'BTC/USD:BTC' -> 'BTC_USD_BTC'
    if not symbol:
        return 'UNKNOWN'
    return symbol.replace('/', '_').replace(':', '_')

def load_config(config_path):
    """Load an YAML configuration file."""
    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found at {config_path}")
        return None
        
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading config {config_path}: {e}")
        return None
