import requests
import time
import pandas as pd


class BinanceAPI():
    def __init__(self) -> None:
        self.api_root = r"https://api.binance.com"

    def get_klines(self, symbol, end_time, limit=1000):
        url = self.api_root + f"/api/v3/klines?symbol={symbol}&interval=1m&endTime={end_time}&limit={limit}"
        res = self.__get_request(url).json()
        res = self.__format_kline(res)
        return res

    def __get_request(self, url):
        tries, max_tries = 0, 10
        while True:
            res = requests.get(url)
            if tries > max_tries:
                raise Exception("Max tries exceeded")
            elif res.status_code != 200:
                time.sleep(5)
                tries += 1
                continue
            else:
                break
        return res

    def __format_kline(self, klines):
        df = pd.DataFrame(klines, columns=["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume", "num_trades", "taker_buy_volume", "taker_buy_quote_volume", "ignore"])
        df = df.drop(labels=["close_time", "ignore"], axis=1)
        df = df.apply(pd.to_numeric, errors="coerce")
        df = df.set_index("open_time")
        return df
