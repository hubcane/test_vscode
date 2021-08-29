from urllib import parse 
from ast import literal_eval 
import requests 
from pprint import pprint
import pandas as pd
import datetime as dt

def get_sise(code, thistime) : 
    get_param = { 
        'code' : code,
        'thistime' : thistime 
    } 
    get_param = parse.urlencode(get_param) 
    url="https://api.finance.naver.com/sise_time.nhn?%s"%(get_param)

    
    print()
    print(url)
    print()

    response = requests.get(url)
    resp_val = literal_eval(response.text.strip())

    returnval = pd.DataFrame(resp_val[1:], columns=resp_val[0]) 
    return returnval

if __name__ == '__main__':
    df = get_sise('005930', '20210817150000')
