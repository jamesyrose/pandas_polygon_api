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

        :param date: datetime
        :param ticker:  str
        :param rate_limit: int (max 50,000)
        :return:
        """
        time_offset = 0
        date_str = date.strftime("%Y-%m-%d")
        data = pd.DataFrame()
        working = True
        while working:
            end_point = f"https://api.polygon.io/v2/ticks/stocks/trades/{ticker}/{date_str}" \
                        f"?apiKey={cls.API_KEY}?timestamp={time_offset}&limit={rate_limit}"
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

