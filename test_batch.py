from crawl_pykrx_try import get_master_ticker_code
import os
import pandas as pd
from collections import Counter, defaultdict
import datetime as dt
import psycopg2 as pg
import gzip
import _pickle as pickle

class run():
    def __init__(self) -> None:
        self.strToday = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')
        pass

    def connect(self):
        fn = 'dbinfo.data'
        fp = gzip.open(fn)
        info = pickle.load(fp)
        fp.close()

        conn = pg.connect(
            host = info['host'],
            user = info['user'],
            password = info['password'],
            port = info['port'],
            database = info['db']
            )
        self.conn = conn
        self.curs = conn.cursor()
        return None

    def write_error(self, log, filename, path = os.getcwd()):
        os.chdir(path)
        fp = open(filename, 'a')
        sentence = '\t'.join([dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S'), log, '\n'])
        fp.write(sentence)
        fp.close()
        return None

    def str_convert(self, stra):
        return "\'" + str(stra) + "\'"


    def disconnect(self):
        self.conn.close()
        return None

    def insert(self):
        marketList = ['KOSPI', 'KOSDAQ', 'KONEX']
        for market in marketList:
            sr = get_master_ticker_code(strdate = self.strToday, market = market)
            dicta = sr.to_dict()
            market = self.str_convert(market)

            for corp_code, corp_name in dicta.items():
                corp_code = self.str_convert(corp_code)
                corp_name = self.str_convert(corp_name)
                strToday = self.str_convert(self.strToday)
                boolval = self.str_convert(True)
                key_dt = strToday

                sql = f'insert into ms_corp_code(corp_code, corp_name, market, create_dt, chg_dt, is_surv, key_dt) values ({corp_code}, {corp_name}, {market}, {strToday}, {strToday}, {boolval}, {key_dt});'
                try:
                    self.curs.execute(sql)
                except Exception as e:
                    self.write_error(str(e), 'insertErrorLog.txt')
                self.conn.commit()

if __name__ == '__main__':
    r = run()
    r.connect()
    r.insert()
    r.disconnect()