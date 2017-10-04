import numpy
import pandas
from pandas_feather_chunk.pandas_feather_chunk import PandasFeatherChunkWriter,\
    PandasFeatherChunkReader
from numpy.testing.utils import assert_array_almost_equal
from nose.tools import assert_list_equal
import os


def test_read_write():
    dfs = [pandas.DataFrame(numpy.random.normal(size=(100,10)), columns=['x%d' % i for i in range(10)]) for _ in range(10)]
    assert not os.path.exists('testfile.tar')
    writer = PandasFeatherChunkWriter('testfile.tar')
    for df in dfs:
        writer.write_chunk(df)
    writer.close()
    reader = PandasFeatherChunkReader('testfile.tar')
    reader.close()
    os.remove('testfile.tar')
    new_dfs = list(reader)
    for df, new_df in zip(dfs, new_dfs):
        assert_array_almost_equal(numpy.asarray(df), numpy.asarray(new_df))
        assert_list_equal(list(df.columns), list(new_df.columns))
    
    
    
    
if __name__ == '__main__':
    import sys
    import nose
    # This code will run the test in this file.'
    module_name = sys.modules[__name__].__file__

    result = nose.run(argv=[sys.argv[0],
                            module_name,
                            '-s', '-v'])