from binance_api import BinanceAPI
from datetime import datetime as dt, timedelta as td, timezone as tz, tzinfo
from pathlib import Path
import pickle
import time


class BinanceDataManager():
    def __init__(self):
        self.api = BinanceAPI()
        self.data_folder = "./data"

    def update_new_klines(self, symbol):
        fpath = self.data_folder + f"/{symbol}/klines.pkl"
        if Path(fpath).is_file():  # intialise data
            data = self.load_data(fpath=fpath)
        else:
            tnow = int(dt.now().timestamp()//60*60*1000)
            data = self.api.get_klines(symbol, tnow)

        tmax = data.index.max()  # get dates
        tnow = int(dt.now().timestamp()//60*60*1000)
        new_klines = self.__get_range_of_klines(symbol, tmax, tnow)  # get new klines
        updated_data = new_klines.combine_first(data)  # insert and stage for writing
        self.__write_data(updated_data, fpath)  # write

    def update_old_klines(self, symbol, tstart):
        fpath = self.data_folder + f"/{symbol}/klines.pkl"
        if Path(fpath).is_file():  # intialise data
            data = self.load_data(fpath=fpath)
        else:
            tnow = int(dt.now().timestamp()//60*60*1000)
            data = self.api.get_klines(symbol, tnow)

        tend = data.index.min() + 60000
        # TODO: find way of automatically finding the earliest time that market exists for on binance
        # tstart = (dt(2017, 9, 1, tzinfo=tz.utc)-dt(1970, 1, 1, tzinfo=tz.utc)).total_seconds() * 1000
        new_klines = self.__get_range_of_klines(symbol, tstart, tend)  # get new klines
        updated_data = new_klines.combine_first(data)  # insert and stage for writing
        self.__write_data(updated_data, fpath)  # write

    def __get_range_of_klines(self, symbol, tstart, tend):
        new_klines = self.api.get_klines(symbol, tend)
        i = 0
        loop_checkpoint = time.time()
        while new_klines.index.min() > tstart:
            t = new_klines.index.min() + 60000
            d = self.api.get_klines(symbol, t)
            new_klines = d.combine_first(new_klines)
            i += 1
            if i % 10 == 0:
                tleft = new_klines.index.min() - tstart  # time left in ms
                tloopleft = tleft / (10 * 1000 * 60 * 1000 / (time.time() - loop_checkpoint))
                print(f"Up to {dt.now().fromtimestamp(t//1000)}... {int(tloopleft)}s left")
                loop_checkpoint = time.time()

        return new_klines

    def __write_data(self, var, fpath):
        with open(fpath, "wb") as f:
            pickle.dump(var, f, protocol=pickle.HIGHEST_PROTOCOL)
        return

    def load_data(self, fpath):
        with open(fpath, "rb") as f:
            data = pickle.load(f)
        return data
