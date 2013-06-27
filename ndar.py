import sys
import os
import errno
import tempfile
import csv
import shutil
import subprocess
import zipfile
try:
    from boto.s3.connection import OrdinaryCallingFormat, S3Connection
    import boto.s3.key
    from boto.exception import S3ResponseError
except ImportError:
    pass
try:
    import MySQLdb
except ImportError:
    pass

image03_attributes = ('acquisition_matrix', 'collection_id', 
                      'collection_title', 'comments_misc', 'dataset_id', 
                      'decay_correction', 'extent4_type', 'extent5_type', 
                      'flip_angle', 'frame_end_times', 'frame_end_unit', 
                      'frame_start_times', 'frame_start_unit', 'gender', 
                      'image_description', 'image_extent1', 'image_extent2', 
                      'image_extent3', 'image_extent4', 'image_extent5', 
                      'image_file', 'image_file_format', 'image_history', 
                      'image_modality', 'image_num_dimensions', 
                      'image_orientation', 'image_resolution1', 
                      'image_resolution2', 'image_resolution3', 
                      'image_resolution4', 'image_resolution5', 
                      'image_slice_thickness', 'image_thumbnail_file', 
                      'image_unit1', 'image_unit2', 'image_unit3', 
                      'image_unit4', 'image_unit5', 'interview_age', 
                      'interview_date', 'magnetic_field_strength', 
                      'mri_echo_time_pd', 'mri_field_of_view_pd', 
                      'mri_repetition_time_pd', 'patient_position', 
                      'pet_isotope', 'pet_tracer', 'photomet_interpret', 
                      'qc_description', 'qc_fail_quest_reason', 'qc_outcome', 
                      'receive_coil', 'scanner_manufacturer_pd', 
                      'scanner_software_versions_pd', 'scanner_type_pd', 
                      'src_subject_id', 'subjectkey', 
                      'time_diff_inject_to_image', 'time_diff_units', 
                      'transformation_performed', 'transformation_type', 
                      'transmit_coil')

def NDARError(Exception):
    """base class for exceptions"""

def ObjectNotFoundError(NDARError):

    """S3 object not found"""

    def __init__(self, object):
        self.object = object
        return

    def __str__(self):
        return 'Object not found: %s' % self.object

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

class _BaseImage(object):

    """base class for images"""

    def __init__(self, image03_dict=None):
        # this is set to true once the temporary directory has been 
        # created -- we just need it set now in case we fail before then 
        # and __del__() is called
        self._clean = False
        self._tempdir = tempfile.mkdtemp()
        self._clean = True
        if image03_dict:
            self._set_image03_attributes(image03_dict)
        self.nifti = None
        return

    def __getattribute__(self, name):
        value = object.__getattribute__(self, name)
        if name == 'nifti' and value is None:
            if self.files['NIfTI-1']:
                value = self.files['NIfTI-1']
            elif self.files['DICOM'] \
                 or self.files['MINC'] \
                 or self.files['BRIK'] \
                 or self.files['NRRD']:
                if self.files['DICOM']:
                    source = self.files['DICOM'][0]
                elif self.files['MINC']:
                    source = self.files['MINC'][0]
                elif self.files['BRIK']:
                    source = '%s.BRIK' % self.files['BRIK'][0]
                else:
                    source = self.files['NRRD'][0]
                value = '%s/image.nii.gz' % self._tempdir
                fo_out = open('%s/mc_stdout' % self._tempdir, 'w')
                fo_err = open('%s/mc_stdout' % self._tempdir, 'w')
                args = ['mri_convert', self.path(source), value]
                try:
                    rv = subprocess.call(args, stdout=fo_out, stderr=fo_err)
                except:
                    fo_out.close()
                    fo_err.close()
                if rv != 0:
                    raise AttributeError('mri_convert call failed')
                self.nifti = value
            else:
                raise AttributeError('image is not a volume')
        return value

    def _set_image03_attributes(self, image03_dict):
        """set the image03 attributes from the passed dictionary
        because we don't know what NDAR might actually be passing as 
        variable names, we only pull out the ones we know (and won't conflict 
        with other attributes)
        """
        for attr in image03_attributes:
            val = image03_dict[attr]
            # values from packages on disk may contain empty strings for 
            # missing values; convert to None here
            if not val:
                val = None
            setattr(self, attr, val)
        return

    def _unpack(self):

        os.mkdir('%s/unpacked' % self._tempdir)
        self.files = {'DICOM': [], 
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
                self.files[_get_file_type(full_path)].append(fname)
        else:
            file_type = _get_file_type(self.source)
            self.files[file_type].append(self._source_base)
            os.symlink(self._temp_source, self.path(self._source_base))

        # now match up .HEADs and .BRIKs
        heads = set([ name[:-5] for name in self.files['BRIK'] \
                                if name.endswith('.HEAD') ])
        briks = set([ name[:-5] for name in self.files['BRIK'] \
                                if name.endswith('.BRIK') ])
        pairs = heads.intersection(briks)
        self.files['BRIK'] = list(pairs)
        lone_heads = [ base+'.HEAD' for base in heads-pairs ]
        lone_briks = [ base+'.BRIK' for base in briks-pairs ]
        self.files['other'].extend(lone_heads)
        self.files['other'].extend(lone_briks)

        # sort the file names
        for l in self.files.itervalues():
            l.sort()

        return

    def __del__(self):
        self.close()
        return

    def exists(self):
        """report whether the file or S3 object exists

        must be defined in subclasses
        """
        raise NotImplementedError()

    def close(self):
        """Clean up temporary files."""
        if self._clean:
            shutil.rmtree(self._tempdir)
        return

    def path(self, fname):
        """Return the full path to a single file."""
        return '%s/unpacked/%s' % (self._tempdir, fname)

class Image(_BaseImage):

    """image-from-file class"""

    def __init__(self, source, image03_dict=None, check_existence=False):
        _BaseImage.__init__(self, image03_dict)
        self.source = os.path.abspath(source)
        if check_existence:
            if not self.exists():
                raise IOError(errno.ENOENT, 
                              "No such file or directory: '%s'" % self.source)
        self.files = None
        return

    def __getattribute__(self, name):
        value = _BaseImage.__getattribute__(self, name)
        if name == 'files' and value is None:
            self._source_base = os.path.basename(self.source)
            self._temp_source = '%s/%s' % (self._tempdir, self._source_base)
            os.symlink(self.source, self._temp_source)
            self._unpack()
            return self.files
        return value

    def exists(self):
        """report whether the file or S3 object exists"""
        return os.path.exists(self.source)

class S3Image(_BaseImage):

    """image-from-S3 class"""

    def __init__(self, 
                 source, s3_access_key, s3_secret_key, 
                 image03_dict=None, check_existence=False):
        _BaseImage.__init__(self, image03_dict)
        if 'boto' not in sys.modules:
            raise ImportError('boto S3 connection module not found')
        self.source = source
        self._s3_access_key = s3_access_key
        self._s3_secret_key = s3_secret_key
        if check_existence:
            if not self.exists():
                raise ObjectNotFoundError(source)
        self.files = None
        return

    def __getattribute__(self, name):
        value = _BaseImage.__getattribute__(self, name)
        if name == 'files' and value is None:
            # source is 's3://bucket/path/to/object'
            (bucket_name, object_name) = self.source[5:].split('/', 1)
            s3 = S3Connection(self._s3_access_key, 
                              self._s3_secret_key, 
                              calling_format=OrdinaryCallingFormat())
            bucket = s3.get_bucket(bucket_name)
            key = boto.s3.key.Key(bucket)
            key.key = object_name
            self._source_base = os.path.basename(self.source)
            self._temp_source = '%s/%s' % (self._tempdir, self._source_base)
            key.get_contents_to_filename(self._temp_source)
            s3.close()
            self._unpack()
            return self.files
        return value

    def exists(self):
        """report whether the file or S3 object exists"""
        (bucket_name, object_name) = self.source[5:].split('/', 1)
        s3 = S3Connection(self._s3_access_key, 
                          self._s3_secret_key, 
                          calling_format=OrdinaryCallingFormat())
        try:
            bucket = s3.get_bucket(bucket_name)
        except S3ResponseError, data:
            if data.args[0] == 404:
                s3.close()
                return False
            raise
        key = boto.s3.key.Key(bucket)
        key.key = object_name
        rv = key.exists()
        s3.close()
        return rv

class _BasePackage:

    """base class for packages"""

    def __init__(self):
        return

class Package(_BasePackage):

    """package on disk"""

    def __init__(self, path):
        _BasePackage.__init__(self)
        self.path = path
        self.images = []
        fo = open('%s/image03.txt' % self.path)
        r = csv.reader(fo, delimiter='\t')
        headers = [ el.replace('.', '_') for el in r.next() ]
        # unused
        description = r.next()
        self.images = []
        for row in r:
            row_dict = dict(zip(headers, row))
            image_path = '%s/image03/%s' % (path, row_dict['image_file'])
            self.images.append(Image(image_path, row_dict))
        fo.close()
        return

class MySQLPackage(_BasePackage):

    """package from a mysql database"""

    def __init__(self, 
                 db_host, db_user, db_password, 
                 database, s3_access_key, s3_secret_key):
        if 'MySQLdb' not in sys.modules:
            raise ImportError('MySQLdb module not found')
        _BasePackage.__init__(self)
        db = MySQLdb.connect(db_host, db_user, db_password, database)
        c = db.cursor()
        c.execute('SELECT * FROM image03')
        cols = [ el[0].lower() for el in c.description ]
        self.images = []
        for row in c:
            row_dict = dict(zip(cols, row))
            im = S3Image(row_dict['image_file'], 
                         s3_access_key, 
                         s3_secret_key, 
                         row_dict)
            self.images.append(im)
        db.close()
        return

# eof
