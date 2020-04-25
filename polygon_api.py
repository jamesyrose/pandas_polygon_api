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

    def snap_shot_single(self, ticker):
        """
        Snap shot of current symbol
        :param ticker: ticker
        :return: pd.DataFrame.MultiIndex
        """
        end_point = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/" \
                    f"{ticker}?apiKey={self.API_KEY}"
        content = reuqests.get(end_point)
        return self._multilevel_df(content)

    def get_symbols(self, type="all", market="all",
                    search=None, locale='us', limit=None, active=True):
        """
        Gets tickers and general information on them

        :param type: str - type of stock
        :param market: str -  market type
        :param search: str - conditional word search
        :param locale: str - us/g ( US exchanges, global exchanges)
        :param limit: str - response limit
        :param active: bool - active stocks or inactive stocks
        :return: pd.DataFrame
        """
        def unpack_codes(codes):
            new_codes = {}
            if isinstance(codes, dict):
                for k, v in codes.items():
                    new_codes[f"code_{k}"] = v
                return pd.Series(new_codes)
            else:
                return pd.Series()
        end_point = f"https://api.polygon.io/v2/reference/tickers"
        if type.lower() in ["etp", 'cs', 'adr', 'nvdr', 'gdr', 'index', 'etn', 'etf']:
            type_str = f"?type={type.lower()}"
        else:
            type_str = ""
        if market.lower() in ['stocks', 'indicies', 'crypto', 'fx', 'bonds', 'mf', 'mmf']:
            market_str = f"?market={market.lower()}"
        else:
            market_str = ""
        if search is not None:
            search_str = f"?search={search}"
        else:
            search_str = ""
        if locale.lower() in ['us', 'g']:
            local_str = f"?locale={locale}"
        else:
            local_str = ""
        if active:
            active_str = "true"
        else:
            active_str = 'false'
        page_count = 1
        working = True
        data = pd.DataFrame()
        while working:
            url = f"{end_point}?apiKey={self.API_KEY}{type_str}{market_str}{search_str}{local_str}" \
                  f"&perpage=50&page={page_count}&active={active_str}"
            content = requests.get(url=url)
            ticker_data = content.json()["tickers"]
            if len(ticker_data) == 0:
                working = False
            df = pd.DataFrame(ticker_data)
            df = df.merge(df.codes.apply(lambda c: unpack_codes(c)),
                          left_index=True,
                          right_index=True
                          ).drop(columns="codes")
            data = pd.concat([data, df], axis=0)
            page_count += 1
            if limit is not None:
                if len(data) >= limit:
                    working = False
        return data

    def get_ticker_details(self, ticker: str):
        """
        Gets more details on different companies

        :param ticker: str - ticker symbol
        :return: pd.DataFrame
        """
        end_point = f"https://api.polygon.io/v1/meta/symbols/{ticker.upper()}/company?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        data = content.json()
        if "error" in data.keys():
            print(f"{data['error']}: {end_point}")
            raise Exception
        df = pd.DataFrame.from_dict(data, orient="index")
        df = df.reset_index()
        df.columns = ['detail', 'description']
        return df

    def get_ticker_news(self, ticker: str, limit=100):
        """
        Gets the news  for given symbol

        :param ticker:
        :param limit: article limit
        :return: pd.DataFrame
        """
        end_point = f"https://api.polygon.io/v1/meta/symbols/{ticker.upper()}/news?apiKey={self.API_KEY}?perpage=50"
        results = pd.DataFrame()
        working = True
        page_cnt = 1
        while working:
            url = f"{end_point}&page={page_cnt}"
            content = requests.get(url=url)
            data = content.json()
            df = pd.DataFrame(data)
            if len(df) < 1:
                working = False
            df['symbols'] = df.symbols.apply(lambda x: x[0])
            results = pd.concat([results, df], axis=0)
            if len(results) >= limit:
                working = False

        return results.sort_values(by=["timestamp"]).reset_index(drop=True)

    def get_split_dates(self, ticker: str):
        """
        Gets the split dates for different symbols
        :param ticker:  symbols
        :return: pd.DataFrame
        """
        end_point = f"https://api.polygon.io/v2/reference/splits/{ticker}?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        data = content.json()["results"]
        return pd.DataFrame(data)

    def get_dividends(self, ticker: str):
        """
        Gets the dividends for different symbols
        :param ticker:  symbols
        :return: pd.DataFrame
        """
        end_point = f"https://api.polygon.io/v2/reference/dividends/{ticker}?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        data = content.json()["results"]
        return pd.DataFrame(data)

    def get_financials(self, ticker: str):
        """
        Gets the financials for different symbols
        :param ticker:  symbols
        :return: pd.DataFrame
        """
        end_point = f"https://api.polygon.io/v2/reference/financials/{ticker}?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        data = content.json()["results"]
        return pd.DataFrame(data)

    def get_historic_trades(self, ticker, dates=[datetime.now()]):
        """
        Get historic trade data

        :param ticker:  symbols
        :param dates:  list of dates
        :return:
        """
        dates = self._keep_trading_days(dates)
        if len(dates) == 0:
            print("No business days entered")
            raise
        pool = mp.Pool(mp.cpu_count() - 2)
        historic_trades = pool.map(partial(self.mp_util.historic_trades_mp, ticker=ticker), dates)
        return pd.concat(historic_trades, axis=0).sort_values("SIP_Time")

    def get_historic_quotes(self, ticker, dates=[datetime.now()]):
        """
        Historic NBBO quotes

        :param ticker:
        :param dates:
        :return: pd.DataFrame
        """
        dates = [date for date in dates
                 if date not in self.us_holidays and   # remove holidays
                 date.isoweekday() in range(1, 6)]  # remove weekends
        if len(dates) == 0:
            print("No business days entered")
            raise
        pool = mp.Pool(mp.cpu_count() - 2)
        historic_quotes = pool.map(partial(self.mp_util.historic_quotes_mp, ticker=ticker), dates)
        return pd.concat(historic_quotes, axis=0).sort_values("SIP_Time")

    def get_last_trade(self, ticker):
        """
        Gets the last confirmed trade

        :param ticker: str
        :return: dict
        """
        end_point = f"https://api.polygon.io/v1/last/stocks/{ticker}?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        data = content.json()
        return data["last"]

    def get_last_quote(self, ticker):
        """
        Gets the last quote

        :param ticker: str
        :return: dict
        """
        end_point = f"https://api.polygon.io/v1/last_quote/stocks/{ticker}?apiKey={self.API_KEY}"
        content = requests.get(end_point)
        data = content.json()
        return data["last"]

    def get_daily_open_close(self, ticker, date=datetime.now()):
        """
        Gets open and close of a given date

        :param ticker: str
        :param date: datetime
        :return: dict
        """
        end_point = f"https://api.polygon.io/v1/open-close/{ticker}/" \
                    f"{date.strftime('%Y-%m-%d')}?apiKey={self.API_KEY}"
        print(end_point)
        content = requests.get(end_point)
        data = content.json()
        return data

    def get_previous_close(self, ticker, unadjusted=False):
        """
        Gets the previous days close for given ticker
        :param ticker: str
        :param unadjusted: bool
        :return: pd.DataFrame
        """
        end_point = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev" \
                    f"?unadjusted={str(unadjusted).lower()}" \
                    f"&apiKey={self.API_KEY}"
        content = requests.get(end_point)
        data = content.json()["results"]
        return pd.DataFrame(data)
