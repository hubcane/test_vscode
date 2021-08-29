import _pickle as pickle
import gzip
import psycopg2 as pg

info = {
    'user': 'js',
    'password' : 'P@ssw0rd',
    'db': 'kstock',
    'port': 5432,
    'host': '221.151.96.233'
}

fn = 'dbinfo.data'
fp = gzip.open(fn, 'w')
pickle.dump(info, fp)
fp.close()

if __name__ == '__main__':
    conn = pg.connect(
        host = info['host'],
        user = info['user'],
        password = info['password'],
        port = info['port'],
        database = info['db']
    )
    conn.close()
    print('done')