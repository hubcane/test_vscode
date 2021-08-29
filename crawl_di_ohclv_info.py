from urllib import parse 
from ast import literal_eval 
import requests 
from pprint import pprint
import pandas as pd
import datetime as dt

def get_sise(code, start_time, end_time, time_from='day') : 
    get_param = { 
        'symbol':code, 
        'requestType':1, 
        'startTime':start_time, 
        'endTime':end_time, 
        'timeframe':time_from 
    } 
    get_param = parse.urlencode(get_param) 
    url="https://api.finance.naver.com/siseJson.naver?%s"%(get_param) 

    print(url)

    response = requests.get(url)
    resp_val = literal_eval(response.text.strip())

    returnval = pd.DataFrame(resp_val[1:], columns=resp_val[0]) 
    return returnval

def addDays(df):
    dayslist = ['MON', 'TUE', 'WED', 'THURS', 'FIR', 'SAT', 'SUN']    
    fwt = '%Y%m%d'
    df['요일'] = df['날짜'].apply(lambda x: dayslist[dt.datetime.strptime(x, fwt).weekday()])
    return df

def renameCols(df):
    df = df.rename(columns = {
        '날짜' : 'date',
        '시가' : 'openPrice',
        '고가' : 'highPrice',
        '저가' : 'lowPrice',
        '종가' : 'endPrice',
        '거래량' : 'amount',
        '외국인소진율': 'foreignRatio',
        '요일' : 'days'
    })
    return df
        
if __name__ == '__main__':
    df = get_sise('005930', '20000601', '20210605', 'day')
    df = addDays(df)
    df = renameCols(df)
    print(df.head())