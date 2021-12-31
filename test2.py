import os
import _pickle as pickle
import gzip 

fp = gzip.open('dbinfo.data', 'r')
fn = pickle.load(fp)
fp.close()

print(fn)