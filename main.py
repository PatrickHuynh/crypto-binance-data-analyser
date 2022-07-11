from binance_data_manager import BinanceDataManager
from datetime import datetime as dt, timedelta as td, timezone as tz, tzinfo
import pickle
from pathlib import Path

b = BinanceDataManager()
b.update_new_klines("BTCUSDT")

# symbol = "BTCUSDT"
# fpath = f"./data/{symbol}/klines.pkl"
# data = None
# if Path(fpath).is_file():  # intialise data
#     with open(fpath, "rb") as f:
#         data = pickle.load(f)

pass
