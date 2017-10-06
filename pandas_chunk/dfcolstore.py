import shelve
from sklearn.externals import joblib
from io import BytesIO
import pandas
import anydbm as dbm

class KeyListKey(object):
    pass

def joblib_str(obj):
    buf = BytesIO()
    joblib.dump(obj, buf)
    return buf.getvalue()

def joblib_obj(data):
    buf = BytesIO(data)
    buf.seek(0)
    obj = joblib.load(buf)
    return obj

def dfcolwrite(df, path):
    db = dbm.open(path, 'c')
    for key in df.columns:
        col = df[key]
        db[joblib_str(key)] = joblib_str(col)
    db.close()

def dfcolread(path, cols=None):
    store = dbm.open(path, 'c')
    if cols is None:
        keys = store.keys()
    else:
        keys = map(joblib_str, cols)
    result = pandas.DataFrame()
    for key in keys:
        result[joblib_obj(key)] = joblib_obj(store[key])
    store.close()
    return result

    