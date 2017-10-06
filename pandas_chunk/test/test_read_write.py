import numpy
import pandas
from pandas_chunk.pandas_chunk import PandasChunkWriter,\
    PandasChunkReader, PandasBufferingStreamWriter,\
    PandasBufferingStreamReader
from numpy.testing.utils import assert_array_almost_equal
from nose.tools import assert_list_equal
import os
from nose import SkipTest
from pandas.io.pytables import read_hdf
from sklearn.externals import joblib
from pandas_chunk.dfcolstore import dfcolwrite, dfcolread
from pandas.util.testing import assert_frame_equal
import time

@SkipTest
def test_dfcolstore():
    assert not os.path.exists('storage.df')
    assert not os.path.exists('storage.jbl')
    try:
        n = 10000
        df = pandas.DataFrame(numpy.random.normal(size=(100,n)), columns=['x%d' % i for i in range(n)])
        t0 = time.time()
        dfcolwrite(df, 'storage.df')
        df2 = dfcolread('storage.df')
        t1 = time.time()
        for col in df.columns:
            joblib.dump(df[col], 'storage.jbl')
            joblib.load('storage.jbl')
        t2 = time.time()
        print t2 - t1, t1 - t0
#         hdf = pandas.HDFStore('storage.h5')
#         hdf.put('d1', df, format='table', data_columns=True)
#         hdf.close()
#         df2 = read_hdf('storage.h5', 'd1', columns=['x5','x3'])
        assert_frame_equal(df, df2[df.columns], check_names=True)
    finally:
        os.remove('storage.df')
        os.remove('storage.jbl')

@SkipTest
def test_hdf():
    assert not os.path.exists('storage')
    try:
        n = 10000
        df = pandas.DataFrame(numpy.random.normal(size=(10000,n)), columns=['x%d' % i for i in range(n)])
        with open('storage', 'wb') as outfile:
            joblib.dump(df, outfile)
        with open('storage', 'rb') as infile:
            df2 = joblib.load(infile)
#         hdf = pandas.HDFStore('storage.h5')
#         hdf.put('d1', df, format='table', data_columns=True)
#         hdf.close()
#         df2 = read_hdf('storage.h5', 'd1', columns=['x5','x3'])
        assert (df2 == df).all().all()
    finally:
        os.remove('storage')
    
def test_chunk_read_write():
    assert not os.path.exists('testfile.tar')
    try:
        dfs = [pandas.DataFrame(numpy.random.normal(size=(10000,10)), columns=['x%d' % i for i in range(10)]) for _ in range(10)]
        writer = PandasChunkWriter('testfile.tar')
        for df in dfs:
            writer.write_chunk(df)
        writer.close()
        reader = PandasChunkReader('testfile.tar')
        new_dfs = list(reader)
        reader.close()
        for df, new_df in zip(dfs, new_dfs):
            assert_array_almost_equal(numpy.asarray(df), numpy.asarray(new_df))
            assert_list_equal(list(df.columns), list(new_df.columns))
    finally:
        os.remove('testfile.tar')
    
def test_buffering_read_write():
    assert not os.path.exists('testfile.tar')
    try:
        df = pandas.DataFrame(numpy.random.normal(size=(10000,10)), columns=['x%d' % i for i in range(10)])
        writer = PandasBufferingStreamWriter('testfile.tar', max_chunk_cells=100000)
        for _, row in df.iterrows():
            writer.write_row(row)
        writer.close()
        reader = PandasBufferingStreamReader('testfile.tar')
        new_df = pandas.DataFrame(list(reader))
        assert_array_almost_equal(numpy.asarray(df), numpy.asarray(new_df))
        assert_list_equal(list(df.columns), list(new_df.columns))
    finally:
        os.remove('testfile.tar')
                          
    
if __name__ == '__main__':
    import sys
    import nose
    # This code will run the test in this file.'
    module_name = sys.modules[__name__].__file__

    result = nose.run(argv=[sys.argv[0],
                            module_name,
                            '-s', '-v'])