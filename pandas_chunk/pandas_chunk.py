'''
The file format is just a tarball full of joblib files.  No 
checking is done for consistency of the naming scheme or the chunk schemas.  
'''
from tarfile import TarFile, TarInfo
import pandas
from io import BytesIO
from sklearn.externals import joblib
from toolz.functoolz import identity

def joblib_str(obj):
    buf = BytesIO()
    joblib.dump(obj, buf)
    return buf.getvalue()

def joblib_obj(data):
    buf = BytesIO(data)
    buf.seek(0)
    obj = joblib.load(buf)
    return obj

class PandasChunkObject(object):
    def __init__(self, filename_or_buffer):
        self.filename_or_buffer = filename_or_buffer
    
    def close(self):
        self.tarball.close()

class PandasChunkReader(PandasChunkObject):
    def __init__(self, filename_or_buffer, *args, **kwargs):
        PandasChunkObject.__init__(self, filename_or_buffer, *args, **kwargs)
        self.tarball = TarFile.open(self.filename_or_buffer, 'r|gz')
    
    def read_chunk(self):
        try:
            current_tarinfo = self.tarball.next()
        except IOError, e:
            if e.message == 'TarFile is closed':
                raise StopIteration()
            else:
                raise
        if current_tarinfo is None:
            raise StopIteration()
        fileobj = self.tarball.extractfile(current_tarinfo)
        chunk = joblib.load(BytesIO(fileobj.read()))
        return chunk
    
    def __iter__(self):
        return self
    
    def next(self):
        return self.read_chunk()
    
def df_from_chunks(filename_or_buffer, columns=None, max_chunks=float('inf'), verbose=False):
    reader = PandasChunkReader(filename_or_buffer)
#     result = pandas.DataFrame()
    keys = columns
    result = []
    for i, chunk in enumerate(reader):
        if i > max_chunks:
            break
        if keys is None:
            keys = list(chunk.columns)
        result.append(chunk[keys])# = pandas.concat([result, chunk[keys]])
        if verbose:
            print('Loaded %d chunks' % i)
    
    if verbose:
        print('All chunks loaded.  Concatenating')
    result = pandas.concat(result)
    if verbose:
        print('Concatenation complete.')
        print('Dataframe loading from chunks complete.')
    return result
    
class PandasChunkWriter(PandasChunkObject):
    def __init__(self, filename_or_buffer):
        PandasChunkObject.__init__(self, filename_or_buffer)
        self.tarball = TarFile.open(self.filename_or_buffer, 'w|gz')
        self.chunk = 0
        
    def write_chunk(self, dataframe):
        name = 'chunk_%d' % self.chunk
#         buf = BytesIO()
#         joblib.dump(dataframe, buf)
#         dataframe.to_feather(buf)
        data = joblib_str(dataframe)
#         buf.read()
        current_tarinfo = TarInfo(name)
        current_tarinfo.size = len(data)
        self.tarball.addfile(current_tarinfo, BytesIO(data))
        self.chunk += 1

class PandasBufferingStreamObject(object):
    def __init__(self, filename_or_buffer, max_chunk_cells=100000):
        self.filename_or_buffer = filename_or_buffer
        self.max_chunk_cells = max_chunk_cells
        self.chunk_size = None
        self.buffer = []
    
    def compute_chunk_size_from_row_length(self, row_length):
        return max(int(self.max_chunk_cells) // int(row_length), 1)
    
    def init_from_row(self, row):
        row_length = len(row)
        self.chunk_size = max(int(self.max_chunk_cells) // int(row_length), 1)

class PandasBufferingStreamWriter(PandasBufferingStreamObject):
    def __init__(self, filename_or_buffer, max_chunk_cells=1000000):
        PandasBufferingStreamObject.__init__(self, filename_or_buffer=filename_or_buffer, 
                                                    max_chunk_cells=max_chunk_cells)
        self.writer = PandasChunkWriter(self.filename_or_buffer)
    
    def flush(self):
        if self.buffer:
            df = pandas.DataFrame(self.buffer)
            df.reset_index(drop=True, inplace=True)
            self.writer.write_chunk(df)
            self.buffer = []
        
    def write_row(self, row):
        if self.chunk_size is None:
            self.init_from_row(row)
        self.buffer.append(row)
        if len(self.buffer) >= self.chunk_size:
            self.flush()
    
    def close(self):
        self.flush()
        self.writer.close()
    
class PandasBufferingStreamReader(PandasBufferingStreamObject):
    def __init__(self, filename_or_buffer):
        PandasBufferingStreamObject.__init__(self, filename_or_buffer=filename_or_buffer)
        self.reader = PandasChunkReader(self.filename_or_buffer)
        self.chunk = None
    
    def __iter__(self):
        return self
    
    def next(self):
        return self.read_row()
    
    def read_row(self):
        if self.chunk is None:
            self.chunk = self.reader.next().iterrows()
        try:
            _, row = self.chunk.next()
        except StopIteration:
            self.chunk = self.reader.next().iterrows()
            return self.read_row()
        return row


def convert_csv_to_chunk_format(infilename, outfilename, chunksize=10000, columns=slice(None), nrows=None, verbose=False, transformation=identity):    
    reader = pandas.read_csv(infilename, chunksize=chunksize, low_memory=False, nrows=nrows)
    writer = PandasChunkWriter(outfilename)
    for i, chunk in enumerate(reader):
        if verbose:
            print('Converting chunk %d' % i)
        writer.write_chunk(transformation(chunk[columns]))
    writer.close()

