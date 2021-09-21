from urllib import parse 
from ast import literal_eval 
import requests 
from pprint import pprint
import pandas as pd
import datetime as dt
import json

def convert_datetime_unixtime(strdt):
    fwt = '%Y-%m-%d %H:%M:%S'
    dtdate = dt.datetime.strptime(strdt, fwt)
    return int(dtdate.timestamp())
    
def convert_unixtime_datetime(intdt):
    fwt = '%Y-%m-%d %H:%M:%S'
    dtdate = dt.datetime.fromtimestamp(intdt)
    return str(dtdate)

def get_sise(code, start_time, end_time, interval='1m') : 
    get_param = { 
        "symbol": code,
        "period1": convert_datetime_unixtime(start_time), 
        "period2": convert_datetime_unixtime(end_time),
        "useYfid": "true",
        "interval":  interval,
        "includePrePost": "true", 
        "events": "div|split|earn",
        "lang": "en-US",
        "region": "US", 
        "crumb": "ESwQF4O8qJY",
        "corsDomain": "finance.yahoo.com" 
    } 
    get_param = parse.urlencode(get_param) 
    url="https://query1.finance.yahoo.com/v8/finance/chart/%s%s%s"%(code, '?', get_param) 

    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})

    resp_val = json.loads(response.text.strip())
    return resp_val

def convert_to_dict(readjson):
    timestamp = readjson['chart']['result'][0]['timestamp']
    timestamp = [convert_unixtime_datetime(t) for t in timestamp]

    datadict = readjson['chart']['result'][0]['indicators']['quote'][0]

    openlist = datadict['open']
    closelist = datadict['close']
    volumelist = datadict['volume']
    highlist = datadict['high']
    lowlist = datadict['low']

    returndict = {}
    returndict['timestamp'] = timestamp
    returndict['open'] = openlist
    returndict['close'] = closelist
    returndict['high'] = highlist
    returndict['low'] = lowlist
    returndict['volume'] = volumelist

    return returndict

if __name__ == "__main__":
    ## max 7 days
    readjson = get_sise("005930.KS", "2021-08-23 00:00:00", "2021-08-24 00:00:00")
    return_dict = convert_to_dict(readjson)
    df = pd.DataFrame(return_dict)
    print(df['timestamp'].values)

    # df = pd.DataFrame(return_dict)
    # print(df.head())