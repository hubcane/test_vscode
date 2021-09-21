import concurrent.futures
from crawl_mi_ohclv_info import get_sise, convert_to_dict
import os
import pandas as pd
from collections import Counter, defaultdict
import datetime as dt
import psycopg2 as pg
import gzip
import _pickle as pickle
from tqdm import tqdm
import time
from dateutil.relativedelta import relativedelta

class run():
    def __init__(self, nowpath, strstart, strend, stockcode, type) -> None:
        os.chdir(nowpath)
        self.path = nowpath
        self.strToday = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')
        self.connect()
        if type == 1:
            self.get_corplist()
            self.disconnect()
            return None
        else: pass
        self.get_results(strstart, strend, stockcode)

        try:
            self.upload_bulk(stockcode)
        except Exception as e:
            self.write_error(str(e), stockcode + '.txt', os.path.join(self.path, 'log'))
            self.upload_individually(stockcode)
        self.disconnect()

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
        sentence = '\t'.join([dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S'), log, '\n'])
        fp.write(sentence)
        fp.close()
        return None

    def get_corplist(self):
        sql = 'select corp_code from ms_corp_code;'
        self.curs.execute(sql)
        results = self.curs.fetchall()
        self.results = [r[0] for r in results]
        return None

    def calculateDay(self, date):
        dtdate = dt.datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S')
        weekday = dtdate.weekday()
        dayslist = ['MON', 'TUE', 'WED', 'THUR', 'FRI', 'SAT', 'SUN']
        return dayslist[weekday]


    def str_convert(self, stra):
        return "\'" + str(stra) + "\'"

    def get_results(self, strstartime, strendtime, stockcode):
        stockcode = stockcode + '.KS'
        readjson = get_sise(stockcode, strstartime, strendtime)
        return_dict = convert_to_dict(readjson)
        df = pd.DataFrame(return_dict)
        df['day'] = df['timestamp'].apply(lambda x: self.calculateDay(x))
        df = df.fillna(0)
        self.df = df
        return None

    def upload_bulk(self, stockcode):
        df = self.df
        keytime = df['timestamp'].values
        day = df['day'].values
        open = df['open'].values
        high = df['high'].values
        close = df['close'].values
        low = df['low'].values
        volume = df['volume'].values
        create_dt = self.str_convert(self.strToday)
        chg_dt = self.str_convert(self.strToday)

        rows = [(kt, d, o, h, c, l, v, create_dt, chg_dt) for kt, d, o, h, c, l, v in zip(keytime, day, open, high, close, low, volume)]
        sql = f'insert into mi_ks{stockcode} (keytime, day, open, close, high, low, volume, create_dt, chg_dt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s);'
        self.curs.executemany(sql, rows)
        self.conn.commit()
        return None

    def upload_individually(self, stockcode):
        for _, rows in self.df.iterrows():
            keytime = self.str_convert(rows['timestamp'])
            day = self.str_convert(rows['day'])            
            
            open = rows['open']

            open = rows['open']
            high = rows['high']
            close = rows['close']
            low = rows['low']
            volume = rows['volume']

            # if not open >= 0:
            #     open, high, close, low, volume = 0, 0, 0, 0, 0
            # else:
            #     open = rows['open']
            #     high = rows['high']
            #     close = rows['close']
            #     low = rows['low']
            #     volume = rows['volume']

            create_dt = self.str_convert(self.strToday)
            chg_dt = self.str_convert(self.strToday)
            sql = f'insert into mi_ks{stockcode} (keytime, day, open, close, high, low, volume, create_dt, chg_dt) values ({keytime}, {day}, {open}, {close}, {high}, {low}, {volume}, {create_dt}, {chg_dt});'
            try:
                self.curs.execute(sql)
                self.conn.commit()
            except Exception as e:
                self.write_error(str(e) + '\t' + sql, stockcode + '.txt', os.path.join(self.path, 'log'))
        return None

    def disconnect(self):
        self.conn.close()
        return None

def insert(inputlist):
    path = inputlist[0]
    strstarttime = inputlist[1]
    strendtime = inputlist[2]
    stockcode = inputlist[3]
    type = inputlist[4]

    t1 = time.time()
    try:
        run(path, strstarttime, strendtime, stockcode, type)
    except Exception as e:
        print(stockcode, e)

    t2 = time.time()
    print(stockcode, t2-t1, 'seconds collapsed')
    return None

if __name__ == "__main__":

    standtime = dt.datetime.now() - relativedelta(days = 2)
    strendtime = dt.datetime.strftime(standtime, '%Y-%m-%d %H:%M:%S')
    strstarttime = dt.datetime.strftime(standtime - relativedelta(days =6), '%Y-%m-%d %H:%M:%S')

    nowpath = os.getcwd()
    inputlist = [nowpath, strstarttime, strendtime, '000020', 1]

    r = run(inputlist[0], inputlist[1], inputlist[2], inputlist[3], inputlist[4])
    stocklist = r.results
    inputlist = [(nowpath, strstarttime, strendtime, stockcode, 2) for stockcode in stocklist]

    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(insert, inputlist)