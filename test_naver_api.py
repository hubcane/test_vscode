import time
import pandas as pd

t1 = time.time()
url='http://finance.naver.com/item/sise_time.nhn?'
df = pd.DataFrame()
for page in range(1, 41):
    pg_url = '{url}&page={page}'.format(url=url, page=page)
    df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

t2 = time.time()

print(df)
print(t2 - t1)