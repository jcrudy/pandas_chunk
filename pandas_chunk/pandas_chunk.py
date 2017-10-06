'''
The file format is just a tarball full of feather files.  No 
checking is done for consistency of the naming scheme or the chunk schemas.  
'''
from tarfile import TarFile, TarInfo
import pandas
from io import BytesIO
from sklearn.externals import joblib

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
        self.tarball = TarFile.open(self.filename_or_buffer, 'r')
    
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
        chunk = joblib.load(fileobj)
        return chunk
    
    def __iter__(self):
        return self
    
    def next(self):
        return self.read_chunk()
    
# class NamedBytesIO(object):
#     def __init__(self, name, *args, **kwargs):
#         self.bytesio = BytesIO(*args, **kwargs)
#         self.name = name
#     
#     def __getattr__(self, item):
#         if item == 'stringio' or item == 'name':
#             return getattr(super(NamedBytesIO, self), item)
#         return getattr(self.bytesio, item)
        
class PandasChunkWriter(PandasChunkObject):
    def __init__(self, filename_or_buffer):
        PandasChunkObject.__init__(self, filename_or_buffer)
        self.tarball = TarFile.open(self.filename_or_buffer, 'w')
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
    def __init__(self, filename_or_buffer, max_chunk_cells=100000, selection=slice(None)):
        self.filename_or_buffer = filename_or_buffer
        self.max_chunk_cells = max_chunk_cells
        self.selection = selection
        self.chunk_size = None
        self.buffer = []
    
    def compute_chunk_size_from_row_length(self, row_length):
        return max(int(self.max_chunk_cells) // int(row_length), 1)
    
    def init_from_row(self, row):
        row_length = len(row)
        self.chunk_size = max(int(self.max_chunk_cells) // int(row_length), 1)
        try:
            row.ix[self.selection]
            self.columns = self.selection
        except:
            self.columns = list(row.index)[self.selection]

class PandasBufferingStreamWriter(PandasBufferingStreamObject):
    def __init__(self, filename_or_buffer, max_chunk_cells=1000000, selection=slice(None)):
        PandasBufferingStreamObject.__init__(self, filename_or_buffer=filename_or_buffer, 
                                                    max_chunk_cells=max_chunk_cells, selection=selection)
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
        selected_row = row.ix[self.columns]
        self.buffer.append(selected_row)
        if self.buffer >= self.chunk_size:
            self.flush()
    
    def close(self):
        self.flush()
        self.writer.close()
    
class PandasBufferingStreamReader(PandasBufferingStreamObject):
    def __init__(self, filename_or_buffer, selection=slice(None)):
        PandasBufferingStreamObject.__init__(self, filename_or_buffer=filename_or_buffer, 
                                                    selection=selection)
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
#         for chunk in self.reader:
#             for _, row in chunk.iterrows():
#                 if self.chunk_size is None:
#                     self.init_from_row(row)
#                 yield row
            
