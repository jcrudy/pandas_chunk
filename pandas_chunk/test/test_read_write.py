import numpy
import pandas
from pandas_chunk.pandas_chunk import PandasChunkWriter,\
    PandasChunkReader, PandasBufferingStreamWriter,\
    PandasBufferingStreamReader, df_from_chunks
from numpy.testing.utils import assert_array_almost_equal
from nose.tools import assert_list_equal
import os
from nose import SkipTest
from pandas.io.pytables import read_hdf
from sklearn.externals import joblib
from pandas.util.testing import assert_frame_equal
import time
from line_profiler import LineProfiler
import pandas_chunk
import pprofile
import sys


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

def test_df_from_chunks():
    assert not os.path.exists('testfile.tar')
    try:
        dfs = [pandas.DataFrame(numpy.random.normal(size=(10000,10)), columns=['x%d' % i for i in range(10)]) for _ in range(10)]
        writer = PandasChunkWriter('testfile.tar')
        for df in dfs:
            writer.write_chunk(df)
        writer.close()
        cols = ['x3', 'x5']
        read_df = df_from_chunks('testfile.tar', cols)
        df = pandas.concat(dfs)
        assert_frame_equal(read_df, df[cols])
    finally:
        os.remove('testfile.tar')

def test_buffering_read_write():
    assert not os.path.exists('testfile.tar')
    try:
        df = pandas.DataFrame(numpy.random.normal(size=(100000,10)), columns=['x%d' % i for i in range(10)])
        writer = PandasBufferingStreamWriter('testfile.tar', max_chunk_cells=1000000)
        for _, row in df.iterrows():
            writer.write_row(row)
        writer.close()
        reader = PandasBufferingStreamReader('testfile.tar')
        new_values = []
        for row in reader:
            new_values.append(row)
        new_df = pandas.DataFrame(new_values)
        assert_array_almost_equal(numpy.asarray(df), numpy.asarray(new_df))
        assert_list_equal(list(df.columns), list(new_df.columns))
    finally:
        os.remove('testfile.tar')
                          
if __name__ == '__main__':
#     prof = pprofile.Profile()
#     with prof():
#         test_buffering_read_write()
#     sys.stdout = open('statsfile.txt', 'w')
#     prof.print_stats()
#     sys.stdout.close()
#     profile = LineProfiler()
#     profile.add_function(test_buffering_read_write)
#     profile.add_module(pandas_chunk)
#     profile.runcall(test_buffering_read_write)
#     profile.print_stats()
    import sys
    import nose
    # This code will run the test in this file.'
    module_name = sys.modules[__name__].__file__
   
    result = nose.run(argv=[sys.argv[0],
                            module_name,
                            '-s', '-v'])