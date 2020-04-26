# Polygon RESTful API using pandas
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](https://spdx.org/licenses/MIT.html)
[![PyPI](https://img.shields.io/pypi/v/pandas_polygon_api.svg?style=flat-square)](https://pypi.python.org/pypi/polygon_pandas_api)
[![](https://img.shields.io/badge/python-3.4+-blue.svg)](https://www.python.org/download/releases/3.4.0/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
### Purpose:
General script to access data from [Polygon](http://polygon.io/) and convert it to pandas dataframe

This is a work in progress
### Notes: 
Currently only supports Stocks.

Only works with [RESTful API](https://polygon.io/docs/#getting-started)

Must have your own API key 

### Install 
```
pip install pandas_polygon_api
```
or
```
git clone https://github.com/jamesyrose/pandas_polygon_api.git
cd pandas_polygon_api-master 
pip install .
```

### Use: 
```
from pandas_polygon_api import PPA

ppa_client = PPA(api_key="<<YOUR_API_KEY>>")
ppa_client.snap_shot_all                      # Snap Shot of whole market (minute aggregation)
ppa_client.get_historic_trade(ticker="SPY", 
                              dates=[datetime(2020, 4, 21), 
                                     datetime(2020, 4, 22)
                                     ]        # Gets all trades taken on SPY on  2020-04-21 and 2020-04-22
ppa_client.get_intraday_bar_agg(ticker="SPY",
                                start_date=datetime(2020, 1, 1),
                                end_date=datetime(2020, 2, 1), 
                                agg_period=1, 
                                unadjusted=False
                                )            # Gets intraday minute bars of SPY for the whole month of January 2020

```

#### All methods
```
exchanges                 # Active Exchanges
get_daily_open_close      # Daily open and close given ticker symbol
get_dividends             # historic dividends of given ticker
get_financials            # Financial information of given ticker
get_full_market_daily_agg # Gets daily candles of ALL symbols for a given date
get_gainers               # Top 20 daily gainers
get_losers                # Top 20 daily losers
get_historic_quotes       # historic quotes of given ticker symbol on given date
get_historic_trades       # historic trades of given ticker symbol on given date
get_intraday_bar_agg      # Gets intraday candles (OHLCV) 
get_last_quote            # Last NBBO quote for given ticker symbol
get_last_trade            # Last completed trade for given ticker symbol
get_locales               # All Locales avaliable
get_markets               # All stock markets avaliable
get_multiple_intraday     # Gets multiple intraday (OHLCV) data for different symbols (pd.MultiIndex)
get_previous_close        # Get prior days close for given ticker
get_split_dates           # Get historic split dates and ratios for given symbol
get_symbols               # Get all symbols avaliable (Be careful with this, there are 80k+ symbols globally)
get_ticker_details        # Information on symbols (e.g. Name, description, industry, etc)
get_ticker_news           # News related to ticker symbol
get_types                 # Types of stocks avliable
is_market_open            # returns  "open" if market is currently open, otherwise "closed"
snap_shot_all             # Gets a snapshot of entire market with minute aggregation
snap_shot_single          # Gets snapshot of single ticker symbol
``` 

 