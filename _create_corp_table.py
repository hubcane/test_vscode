import concurrent
from crawl_pykrx_try import get_master_ticker_code
import os
import pandas as pd
from collections import Counter, defaultdict
import datetime as dt
import psycopg2 as pg
import gzip
import _pickle as pickle
from tqdm import tqdm
import concurrent.futures
import time

class run():
    def __init__(self, path, corp_code) -> None:
        self.strToday = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')
        self.nowpath = path
        os.chdir(self.nowpath)
        self.connect()
        self.get_corplist()
        self.create_table(corp_code)
        self.disconnect()
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

    def write_error(self, log, filename, path):
        os.chdir(path)
        fp = open(filename, 'a')
        sentence = '\t'.join([dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S'), str(log), '\n'])
        fp.write(sentence)
        fp.close()
        return None

    def str_convert(self, stra):
        return "\'" + str(stra) + "\'"

    def disconnect(self):
        self.conn.close()
        return None

    def get_corplist(self):
        # sql = 'select corp_code from ms_corp_code;'
        sql = "select right(relname,6), n_live_tup FROM pg_stat_user_tables where pg_stat_user_tables.schemaname = 'public' and pg_stat_user_tables.n_live_tup = 0;"
        self.curs.execute(sql)
        results = self.curs.fetchall()
        self.results = [r[0] for r in results]
        return None

    def create_table(self, corp_code):
        try:
            sql = f'drop table mi_ks{corp_code};'
            print(sql)
            self.curs.execute(sql)
            self.conn.commit()
        except Exception as e:
            self.write_error(e, corp_code + '.txt', path = os.path.join(self.nowpath, 'log'))

        # try:
        #     sql = f'truncate table mi_ks{corp_code};'
        #     self.curs.execute(sql)
        #     self.conn.commit()
        # except Exception as e:
        #     self.write_error(e, corp_code + '.txt', path = os.path.join(self.nowpath, 'log'))

        # try:
        #     sql = f'create table mi_ks{corp_code} (keytime timestamp, day varchar(4),open float, close float, high float, low float, volume int, create_dt timestamp, chg_dt timestamp, primary key (keytime));'
        #     self.curs.execute(sql)
        #     self.conn.commit()
        # except Exception as e:
        #     self.write_error(e, corp_code + '.txt', path = os.path.join(self.nowpath, 'log'))



        return None

def debug(inputlist):
    path = inputlist[0]
    corp_code = inputlist[1]
    try:
        run(path, corp_code)
    except Exception as e:
        print(e, os.getcwd(), corp_code)

if __name__ == "__main__":
    path = os.getcwd()

    r = run(path, corp_code='00104K')
    corplist = r.results
    print(corplist)

    inputlist = [(path, corp_code) for corp_code in corplist]

    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(debug, inputlist)