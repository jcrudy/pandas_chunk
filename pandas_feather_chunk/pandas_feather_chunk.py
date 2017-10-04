'''
The file format is just a tarball full of feather files.  No 
checking is done for consistency of the naming scheme or the chunk schemas.  
'''
from tarfile import TarFile, TarInfo
import pandas
from cStringIO import StringIO

class PandasFeatherChunkObject(object):
    def __init__(self, filename, *args, **kwargs):
        self.filename = filename
        self.args = args
        self.kwargs = kwargs
        assert 'filepath_or_buffer' not in kwargs
    
    def close(self):
        self.tarball.close()

class PandasFeatherChunkReader(PandasFeatherChunkObject):
    def __init__(self, filename, *args, **kwargs):
        PandasFeatherChunkObject.__init__(self, filename, *args, **kwargs)
        self.tarball = TarFile.open(self.filename, 'r')
    
    def read_chunk(self):
        try:
            current_tarinfo = self.tarball.next()
        except IOError, e:
            if e.message == 'TarFile is closed':
                raise StopIteration()
            else:
                raise
        fileobj = self.tarball.extract(current_tarinfo)
        chunk = pandas.read_feather(fileobj, *self.args, **self.kwargs)
        return chunk
    
    def __iter__(self):
        return self
    
    def next(self):
        return self.read_chunk()
    

class PandasFeatherChunkWriter(PandasFeatherChunkObject):
    def __init__(self, filename, *args, **kwargs):
        PandasFeatherChunkObject.__init__(self, filename, *args, **kwargs)
        self.tarball = TarFile.open(self.filename, 'w')
        self.chunk = 0
        
    def write_chunk(self, dataframe):
        buf = StringIO()
        current_tarinfo = TarInfo(name='chunk_%d' % self.chunk)
        dataframe.to_feather(buf)
        self.tarball.addfile(current_tarinfo, buf)
        self.chunk += 1

