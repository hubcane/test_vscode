from pykrx import stock
from pprint import pprint
import pandas as pd


# ticker code and ticker descritpion
def get_master_ticker_code(strdate = '20210827', market = 'ALL'):
    ticker_code = stock.get_market_ticker_list(date=strdate, market=market)
    ticker_dict = dict()
    
    for ticker in ticker_code:
        ticker_dict[ticker] = stock.get_market_ticker_name(ticker)
        
    sr = pd.Series(ticker_dict)
    return sr

# marekt fundamental information
def get_info_market_fundamental(strdate, market = 'ALL'):
    df = stock.get_market_fundamental_by_ticker(date =strdate, market = 'ALL')
    return df

# marekt fundamental information by ticker
def get_info_market_fundamental_by_ticker(strstartdate, strenddate, tickercode):
    df = stock.get_market_fundamental_by_date(strstartdate, strenddate, tickercode)
    return df

# daily trading amount
# def get_info_daily_trade_amount(strstartdate, strenddate, tickercode, on = '매수'):
    

if __name__ == '__main__':
    print(get_master_ticker_code(market = 'KONEX'))
    # print(get_info_market_fundamental('20210802'))
    # print(get_info_market_fundamental_by_ticker('20210801', '20210827', '005930'))