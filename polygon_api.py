#!usr/bin/python3
import os
import time
import sys
import requests
from functools import partial
from datetime import datetime
import multiprocessing as mp
import pandas as pd
import holidays
from mp_util import MP_Util

class PP_API():
    def __init__(self, api_key):
        self.API_KEY = api_key
        self.mp_util = MP_Util
        self.mp_util.API_KEY = api_key
        self.us_holidays = holidays.UnitedStates()

    def _multilevel_df(self, content):
        """
        Creates multi level data frame for snap_shot/gainers/losers
        :param content:
        :return:
        """
        data = content.json()['tickers']
        if not isinstance(data, list):
            data = [data]
        df = pd.DataFrame(data).set_index("ticker")
        multi_index_dict = {col: df[col].apply(lambda x: pd.Series(x)) for col in df.columns}
        df_multi_index = pd.concat(multi_index_dict.values(), axis=1, keys=multi_index_dict.keys())
        return df_multi_index

    def _keep_trading_days(self, dates):
        """
        removes holidays and weekends from datelist
        """
        dates = [date for date in dates
                 if date not in self.us_holidays and   # remove holidays
                 date.isoweekday()in range(1, 6)]  # remove weekends
        return dates

    @property
    def snap_shot_all(self):
        """
        Gets all symbols current minute agg, daily agg, last_trade,

        :return: pd.MultiIndex, index=ticker
        """
        end_point = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        return self._multilevel_df(content)

    @property
    def get_types(self):
        """
        Gets the types of symbols avaliable
        :return:
        """
        end_point = f"https://api.polygon.io/v2/reference/types?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        types = content.json()["results"]["types"]
        data = pd.DataFrame.from_dict(types, orient="index").reset_index()
        data.columns = ["type", "type_name"]
        return data

    @property
    def get_gainers(self):
        """
        gets the top 20 gainers of the day

        :return: pd.DataFrame.MultiIndex
        """
        end_point = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        return self._multilevel_df(content)

    @property
    def get_losers(self):
        """
        gets the top 20 losers of the day

        :return: pd.DataFrame.MultiIndex
        """
        end_point = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/losers?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        return self._multilevel_df(content)

    @property
    def get_markets(self):
        """
        Gets the markets avaliable
        :return:
        """
        end_point = f"https://api.polygon.io/v2/reference/markets?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        markets = content.json()["results"]
        data = pd.DataFrame(markets)
        return data

    @property
    def get_locales(self):
        """
        Gets the locales avaliable
        :return:
        """
        end_point = f"https://api.polygon.io/v2/reference/locales?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        markets = content.json()["results"]
        data = pd.DataFrame(markets)
        return data

    @property
    def is_market_open(self):
        end_point = f"https://api.polygon.io/v1/marketstatus/now?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        data = content.json()
        return data["market"]

    @property
    def holidays(self):
        end_point = f"https://api.polygon.io/v1/marketstatus/upcoming?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        data = content.json()
        return pd.DataFrame(data)

    @property
    def exchanges(self):
        end_point = f"https://api.polygon.io/v1/meta/exchanges?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        data = content.json()
        return pd.DataFrame(data).drop(columns=['id'])
