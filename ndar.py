import sys
import os
import tempfile
import shutil
import zipfile
try:
    import boto.s3.connection
    import boto.s3.key
except ImportError:
    pass

def _get_file_type(fname):
    """Return the type of a file."""
    if fname.endswith('.nii.gz'):
        return 'NIfTI-1'
    if fname.endswith('.png'):
        return 'PNG'
    if fname.endswith('.jpg'):
        return 'JPEG'
    if fname.endswith('.mnc'):
        return 'MINC'
    if fname.endswith('.nrrd'):
        return 'NRRD'
    if fname.endswith('.HEAD') or fname.endswith('.BRIK'):
        return 'BRIK'
    with open(fname) as fo:
        fo.seek(128)
        if fo.read(4) == 'DICM':
            return 'DICOM'
    return 'other'

class Image:

    """Image class"""

    def __init__(self, source, s3_access_key=None, s3_secret_key=None):

        # this is set to true once the temporary directory has been 
        # created -- we just need it set now in case we fail before then 
        # and __del__() is called
        self._clean = False
        self._tempdir = tempfile.mkdtemp()
        self._clean = True

        if source.startswith('s3://'):
            if not s3_access_key and not s3_secret_key:
                raise TypeError('S3 keys needed for S3 access')
            self._init_from_s3(source, s3_access_key, s3_secret_key)
        else:
            self._init_from_file(source)

        os.mkdir('%s/unpacked' % self._tempdir)
        self.file_dict = {'DICOM': [], 
                          'NIfTI-1': [], 
                          'MINC': [], 
                          'BRIK': [], 
                          'NRRD': [], 
                          'PNG': [], 
                          'JPEG': [], 
                          'other': []}
        if self.source.endswith('.zip'):
            zf = zipfile.ZipFile(self._temp_source)
            zf.extractall('%s/unpacked' % self._tempdir)
            for fname in os.listdir('%s/unpacked' % self._tempdir):
                full_path = '%s/unpacked/%s' % (self._tempdir, fname)
                self.file_dict[_get_file_type(full_path)].append(fname)
        else:
            file_type = _get_file_type(self.source)
            self.file_dict[file_type].append(self._source_base)
            os.symlink(self._temp_source, self.path(self._source_base))

        # now match up .HEADs and .BRIKs
        heads = set([ name[:-5] for name in self.file_dict['BRIK'] \
                                if name.endswith('.HEAD') ])
        briks = set([ name[:-5] for name in self.file_dict['BRIK'] \
                                if name.endswith('.BRIK') ])
        pairs = heads.intersection(briks)
        self.file_dict['BRIK'] = list(pairs)
        lone_heads = [ base+'.HEAD' for base in heads-pairs ]
        lone_briks = [ base+'.BRIK' for base in briks-pairs ]
        self.file_dict['other'].extend(lone_heads)
        self.file_dict['other'].extend(lone_briks)

        # sort the file names
        for l in self.file_dict.itervalues():
            l.sort()

        return

    def _init_from_file(self, source):
        self.source = os.path.abspath(source)
        if not os.path.exists(self.source):
            raise IOError(errno.ENOENT, 
                          "No such file or directory: '%s'" % self.source)
        self._source_base = os.path.basename(self.source)
        self._temp_source = '%s/%s' % (self._tempdir, self._source_base)
        os.symlink(self.source, self._temp_source)
        return

    def _init_from_s3(self, source, s3_access_key, s3_secret_key):
        if 'boto' not in sys.modules:
            raise ImportError('boto S3 connection module not found')
        self.source = source
        # source is 's3://bucket/path/to/object'
        (bucket_name, object_name) = source[5:].split('/', 1)
        calling_format = boto.s3.connection.OrdinaryCallingFormat()
        s3 = boto.s3.connection.S3Connection(s3_access_key, 
                                             s3_secret_key, 
                                             calling_format=calling_format)
        bucket = s3.get_bucket(bucket_name)
        key = boto.s3.key.Key(bucket)
        key.key = object_name
        self._source_base = os.path.basename(self.source)
        self._temp_source = '%s/%s' % (self._tempdir, self._source_base)
        key.get_contents_to_filename(self._temp_source)
        s3.close()
        return

    def __del__(self):
        self.close()
        return

    def close(self):
        """Clean up temporary files."""
        if self._clean:
            shutil.rmtree(self._tempdir)
        return

    def path(self, fname):
        """Return the full path to a single file."""
        return '%s/unpacked/%s' % (self._tempdir, fname)

# eof
