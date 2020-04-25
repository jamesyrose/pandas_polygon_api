#!/usr/bin/python3
import requests
from datetime import timedelta
import pandas as pd


class MP_Util:
    API_KEY = None

    @classmethod
    def historic_trades_mp(cls, date, ticker, rate_limit=50000):
        """
        Gets historic trade for given ticker and date

        queries {rate_limit} from Polygon Restful api > gets last timestampe >
        queries again with timestamps offset > repeats until no new data is being added >
        concatenates all data > return pd.DataFrame

        :param date: (datetime.datetime) - date being queried
        :param ticker:  (str) - ticker symbol
        :param rate_limit: (int) - points per query (max 50,000)
        :return: pd.DataFrame
        """
        time_offset = 0
        date_str = date.strftime("%Y-%m-%d")
        data = pd.DataFrame()
        working = True
        while working:
            end_point = f"https://api.polygon.io/v2/ticks/stocks/trades/{ticker}/{date_str}" \
                        f"?apiKey={cls.API_KEY}&timestamp={time_offset}&limit={rate_limit}"
            content = requests.get(end_point)
            df = pd.DataFrame(content.json()["results"])
            df = df.rename(columns={"t": "SIP_Time",
                                    "y": "Exchange_Time",
                                    "f": "TRF_Time",
                                    "q": "Sequence_Number",
                                    "i": "Trade_ID",
                                    "x": "Exchange_ID",
                                    "s": "size",
                                    "c": "conditions",
                                    "z": "tape_location",
                                    "p": "price"})
            data_len_pre_concat = len(data)
            data = pd.concat([data, df], axis=0).sort_values("SIP_Time")
            data.drop_duplicates(subset=['SIP_Time', "Trade_ID", "size", "price"], inplace=True)
            data_len_post_concat = len(data)
            data['datetime'] = pd.to_datetime(data["SIP_Time"], unit="ns")
            time_offset = data.SIP_Time.max()
            if len(df) < rate_limit or df.empty or data_len_pre_concat == data_len_post_concat:
                working = False
        return data

    @classmethod
    def historic_quotes_mp(cls, date, ticker, rate_limit=50000):
        """
        Gets historic NBBO quotes for given ticker and date

        queries {rate_limit} from Polygon Restful api > gets last timestampe >
        queries again with timestamps offset > repeats until no new data is being added >
        concatenates all data > return pd.DataFrame

        :param date: (datetime.datetime) - date being queried
        :param ticker:  (str) - ticker symbol
        :param rate_limit: (int) - points per query (max 50,000)
        :return: pd.DataFrame
        """
        time_offset = 0
        date_str = date.strftime("%Y-%m-%d")
        data = pd.DataFrame()
        working = True
        while working:
            end_point = f"https://api.polygon.io/v2/ticks/stocks/nbbo/{ticker}/{date_str}" \
                        f"?apiKey={cls.API_KEY}&timestamp={time_offset}&limit={rate_limit}"
            content = requests.get(end_point)
            df = pd.DataFrame(content.json()["results"])
            df = df.rename(columns={"t": "SIP_Time",
                                    "y": "Exchange_Time",
                                    "f": "TRF_Time",
                                    "q": "Sequence_Number",
                                    "c": "conditions",
                                    "z": "tape_location",
                                    "I": "indicators",
                                    "p": "bid",
                                    "x": "bid_Exchange_ID",
                                    "bid_size": "s",
                                    "P": "ask",
                                    "X": "ask_Exchange_ID",
                                    "S": "ask_size"
                                    })
            data_len_pre_concat = len(data)
            data = pd.concat([data, df], axis=0).sort_values("SIP_Time")
            data.drop_duplicates(subset=['SIP_Time', "bid_Exchange_ID", "ask_Exchange_ID"], inplace=True)
            data_len_post_concat = len(data)
            data['datetime'] = pd.to_datetime(data["SIP_Time"], unit="ns")
            time_offset = data.SIP_Time.max()
            if len(df) < rate_limit or df.empty or data_len_pre_concat == data_len_post_concat:
                working = False
        return data

    @classmethod
    def minute_agg_mp(cls, date, ticker, agg_period, unadjusted):
        """
        gets aggregate minutes for one day

        :param date: (datetime.datetime) - date to get the data
        :param ticker: (str) ticker symbol
        :param agg_period: (int) -  aggregation period in minutes
        :param unadjusted: (bool) - True if you DO NOT want to adjust for splits
        :return: pd.DataFrame
        """
        start = date
        end = date + timedelta(days=1)
        end_point = f"https://api.polygon.io/v2/aggs/ticker/{ticker}" \
                    f"/range/{agg_period}/minute/" \
                    f"{start.strftime('%Y-%m-%d')}/" \
                    f"{end.strftime('%Y-%m-%d')}" \
                    f"?unadjusted{str(unadjusted).lower()}" \
                    f"&apiKey={cls.API_KEY}"
        content = requests.get(end_point)
        data = content.json()['results']
        df = pd.DataFrame(data).rename(columns={"v": "volume",
                                                "o": "open",
                                                "c": "close",
                                                "h": "high",
                                                "l": "low",
                                                't': "datetime",
                                                'n': "agg_item_count"}
                                       )
        return df

