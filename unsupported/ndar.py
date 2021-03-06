import sys
import os
import errno
import subprocess
import tempfile
import shutil
import re
import csv
import gzip
import zipfile
import dicom
import nibabel
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
                      'receive_coil', 'scan_type', 'scanner_manufacturer_pd', 
                      'scanner_software_versions_pd', 'scanner_type_pd', 
                      'src_subject_id', 'subjectkey', 
                      'time_diff_inject_to_image', 'time_diff_units', 
                      'transformation_performed', 'transformation_type', 
                      'transmit_coil')

dicom_date_re = re.compile('^(\d\d\d\d)(\d\d)(\d\d)$')

class EXTRACT:
    """bogus class to indicate to image constructors that attributes should 
    be extracted from the image"""

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
    if fname.endswith('.nii.gz') or fname.endswith('.nii'):
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
        return 'AFNI'
    with open(fname) as fo:
        fo.seek(128)
        if fo.read(4) == 'DICM':
            return 'DICOM'
    return 'other'

class _BaseImage(object):

    """base class for images"""

    def __init__(self, attrs=None):
        self._clean_on_del = True
        self._tempdir = None
        self.nifti_1 = None
        self.nifti_1_gz = None
        self.afni = None
        self.xcede = None
        self.nrrd = None
        self.thumbnail = None
        # see _resample_scalar_volume() below
        self._allow_resample_scalar_volume = False
        return

    def _mri_convert(self, source, output):
        fo_out = open('%s/mc_stdout' % self._tempdir, 'w')
        fo_err = open('%s/mc_stdout' % self._tempdir, 'w')
        args = ['mri_convert', self.path(source), output]
        try:
            rv = subprocess.call(args, stdout=fo_out, stderr=fo_err)
        finally:
            fo_out.close()
            fo_err.close()
        return rv == 0

    def _3dcopy(self, source, output):
        fo_out = open('%s/3dcopy_stdout' % self._tempdir, 'w')
        fo_err = open('%s/3dcopy_stdout' % self._tempdir, 'w')
        args = ['3dcopy', source, output]
        try:
            rv = subprocess.call(args, stdout=fo_out, stderr=fo_err)
        finally:
            fo_out.close()
            fo_err.close()
        return rv == 0

    def _dicom2bxh(self, source, output):
        fo_out = open('%s/dicom2bxh_stdout' % self._tempdir, 'w')
        fo_err = open('%s/dicom2bxh_stdout' % self._tempdir, 'w')
        args = ['dicom2bxh', '--xcede']
        args.extend(source)
        args.append(output)
        try:
            rv = subprocess.call(args, stdout=fo_out, stderr=fo_err)
        finally:
            fo_out.close()
            fo_err.close()
        return rv == 0

    def _afni2bxh(self, source, output):
        fo_out = open('%s/afni2bxh_stdout' % self._tempdir, 'w')
        fo_err = open('%s/afni2bxh_stdout' % self._tempdir, 'w')
        args = ['afni2bxh', '--xcede', source, output]
        try:
            rv = subprocess.call(args, stdout=fo_out, stderr=fo_err)
        finally:
            fo_out.close()
            fo_err.close()
        return rv == 0

    def _nrrd2bxh(self, source, output):
        fo_out = open('%s/nrrd2bxh_stdout' % self._tempdir, 'w')
        fo_err = open('%s/nrrd2bxh_stdout' % self._tempdir, 'w')
        args = ['nrrd2bxh', '--xcede', source, output]
        try:
            rv = subprocess.call(args, stdout=fo_out, stderr=fo_err)
        finally:
            fo_out.close()
            fo_err.close()
        return rv == 0

    def _DicomToNrrdConverter(self, source, output):
        fo_out = open('%s/DicomToNrrdConverter_stdout' % self._tempdir, 'w')
        fo_err = open('%s/DicomToNrrdConverter_stdout' % self._tempdir, 'w')
        args = ['DicomToNrrdConverter', 
                '--inputDicomDirectory', 
                source, 
                '--outputDirectory', 
                os.path.dirname(output), 
                '--outputVolume', 
                os.path.basename(output)]
        try:
            rv = subprocess.call(args, stdout=fo_out, stderr=fo_err)
        finally:
            fo_out.close()
            fo_err.close()
        return rv == 0

    def _resample_scalar_volume(self, source, output):
        # I (ch) need this conversion temporarily, but it might not be 
        # appropriate for all scans.  This check makes sure that 
        # this conversion won't be called triggered unless explicitly 
        # requested.
        if not self._allow_resample_scalar_volume:
            return False
        fo_out = open('%s/rss_stdout' % self._tempdir, 'w')
        fo_err = open('%s/rss_stdout' % self._tempdir, 'w')
        args = ['ResampleScalarVolume', self.path(source), output]
        try:
            rv = subprocess.call(args, stdout=fo_out, stderr=fo_err)
        finally:
            fo_out.close()
            fo_err.close()
        return rv == 0

    def __getattribute__(self, name):
        value = object.__getattribute__(self, name)
        if name == '_tempdir' and value is None:
            value = tempfile.mkdtemp()
            self._tempdir = value
        if name == 'nifti_1' and value is None:
            if self.files['NIfTI-1']:
                value = self.path(self.files['NIfTI-1'][0])
            elif self.files['DICOM'] \
                 or self.files['MINC'] \
                 or self.files['AFNI'] \
                 or self.files['NRRD']:
                if self.files['DICOM']:
                    source = self.files['DICOM'][0]
                elif self.files['MINC']:
                    source = self.files['MINC'][0]
                elif self.files['AFNI']:
                    source = '%s.BRIK' % self.files['AFNI'][0]
                else:
                    source = self.files['NRRD'][0]
                value = '%s/image.nii.gz' % self._tempdir
                if self._mri_convert(source, value):
                    pass
                elif self._resample_scalar_volume(source, value):
                    pass
                else:
                    raise AttributeError('conversion to NIfTI-1 failed')
                self.nifti_1 = value
            else:
                raise AttributeError('image is not a volume')
        if name == 'nifti_1_gz' and value is None:
            if self.nifti_1.endswith('.nii.gz'):
                value = self.nifti_1
            else:
                value = '%s/image.nii.gz' % self._tempdir
                fin = open(self.nifti_1)
                fout = gzip.open(value, 'w')
                fout.write(fin.read())
                fout.close()
                fin.close()
            self.nifti_1_gz = value
        if name == 'afni' and value is None:
            if self.files['AFNI']:
                value = self.path(self.files['AFNI'][0])
            elif self.files['DICOM'] \
                 or self.files['MINC'] \
                 or self.files['NIfTI-1'] \
                 or self.files['NRRD']:
                if self.files['DICOM']:
                    source = self.nifti_1
                elif self.files['MINC']:
                    source = self.path(self.files['MINC'][0])
                elif self.files['NIfTI-1']:
                    source = self.path(self.files['NIfTI-1'][0])
                else:
                    source = self.path(self.files['NRRD'][0])
                value = '%s/image+orig' % self._tempdir
                if not self._3dcopy(source, value):
                    raise AttributeError('conversion to AFNI failed')
                self.afni = value
            else:
                raise AttributeError('image is not a volume')
        if name == 'xcede' and value is None:
            if self.files['DICOM'] \
               or self.files['AFNI'] \
               or self.files['MINC'] \
               or self.files['NIfTI-1'] \
               or self.files['NRRD']:
                value = '%s/image.xcede' % self._tempdir
                if self.files['DICOM']:
                    source = [ self.path(f) for f in self.files['DICOM'] ]
                    rv = self._dicom2bxh(source, value)
                elif self.files['AFNI']:
                    source = '%s.HEAD' % self.path(self.files['AFNI'][0])
                    rv = self._afni2bxh(source, value)
                elif self.files['MINC']:
                    rv = self._afni2bxh('%s.HEAD' % self.afni, value)
                elif self.files['NIfTI-1']:
                    rv = self._afni2bxh('%s.HEAD' % self.afni, value)
                else:
                    source = self.path(self.files['NRRD'][0])
                    rv = self._nrrd2bxh(source, value)
                if not rv:
                    raise AttributeError('XCEDE generation failed')
                if not os.path.exists(value):
                    # this could happen if two xcede files are generated; 
                    # for now, panic and bail
                    raise AttributeError('XCEDE generation failed')
                self.xcede = value
            else:
                raise AttributeError('image is not a volume')
        if name == 'nrrd' and value is None:
            if self.files['NRRD']:
                value = self.path(self.files['NRRD'][0])
            elif self.files['DICOM']:
                value = '%s/image.nrrd' % self._tempdir
                source = os.path.dirname(self.path(self.files['DICOM'][0]))
                if not self._DicomToNrrdConverter(source, value):
                    raise AttributeError('DicomToNrrdConverter failed')
            elif self.files['AFNI']:
                raise AttributeError('NRRD conversion for AFNI not supported')
            elif self.files['MINC']:
                raise AttributeError('NRRD conversion for MINC not supported')
            elif self.files['NIfTI-1']:
                raise AttributeError('NRRD conversion for NIfTI-1 not supported')
            else:
                raise AttributeError('image is not a volume')
            self.ndar = value
        if name == 'thumbnail' and value is None:
            value = '%s/thumbnail.png' % self._tempdir
            fo_out = open('%s/slicer_stdout' % self._tempdir, 'w')
            fo_err = open('%s/slicer_stdout' % self._tempdir, 'w')
            args = ['slicer', self.nifti_1, '-a', value]
            try:
                rv = subprocess.call(args, stdout=fo_out, stderr=fo_err)
            finally:
                fo_out.close()
                fo_err.close()
            if rv != 0:
                raise AttributeError('slicer call failed')
            self.thumbnail = value
        return value

    def _set_attributes(self, attrs=None):
        """set attributes as desired

        this should be called at the end of __init__() in derived classes
        """
        if isinstance(attrs, dict):
            self.set_attributes_from_dict(attrs)
        elif attrs == EXTRACT:
            self.extract_attributes()
        return

    def set_attributes_from_dict(self, attrs):
        """set the image03 attributes from the passed dictionary

        because we don't know what NDAR might actually be passing as 
        variable names, we only pull out the ones we know (and won't conflict 
        with other attributes)
        """
        if not isinstance(attrs, dict):
            raise TypeError('argument must be a dictionary')
        for attr in attrs:
            val = attrs[attr]
            # values from packages on disk may contain empty strings for 
            # missing values; convert to None here
            if not val:
                val = None
            setattr(self, attr, val)
        return

    def extract_attributes(self):
        for attr in image03_attributes:
            setattr(self, attr, None)
        if self.files['DICOM']:
            self._extract_attributes_from_dicom()
        elif self.files['NIfTI-1']:
            self.image_file_format = 'NIFTI'
            self._extract_attributes_from_volume()
        elif self.files['MINC']:
            self.image_file_format = 'MINC'
            self._extract_attributes_from_volume()
        elif self.files['AFNI']:
            self.image_file_format = 'AFNI'
            self._extract_attributes_from_volume()
        elif self.files['NRRD']:
            self.image_file_format = 'NRRD'
            self._extract_attributes_from_volume()
        elif self.files['PNG']:
            self.image_file_format = 'PNG'
        elif self.files['JPEG']:
            self.image_file_format = 'JPEG'
        return

    def _extract_attributes_from_volume(self):
        """set image03 attributes that can be extracted from the 
        (format-independent) volume"""
        vol = nibabel.load(self.nifti_1)
        try:
            (xyz_units, t_units) = vol.get_header().xyzt_units()
        except:
            (xyz_units, t_units) = (None, None)
        if xyz_units == 'mm':
            xyz_units = 'Millimeters'
        elif xyz_units == 'm':
            xyz_units = 'Meters'
        elif xyz_units == 'um':
            xyz_units = 'Micrometers'
        else:
            xyz_units = None
        if t_units == 's':
            t_units = 'Seconds'
        elif t_units == 'ms':
            t_unit = 'Milliseconds'
        elif t_units == 'ms':
            t_unit = 'Microseconds'
        else:
            t_unit = None
        self.image_num_dimensions = len(vol.shape)
        pixdim = vol.get_header()['pixdim']
        for i in xrange(self.image_num_dimensions):
            setattr(self, 'image_extent%d' % (i+1), vol.shape[i])
            setattr(self, 'image_resolution%d' % (i+1), pixdim[i+1])
            if i < 3 and xyz_units:
                setattr(self, 'image_unit%d' % (i+1), xyz_unit)
            if i == 3 and t_units:
                self.image_unit4 = t_unit
        return

    def _extract_attributes_from_dicom(self):
        self.image_file_format = 'DICOM'
        self._extract_attributes_from_volume()
        do = dicom.read_file(self.path(self.files['DICOM'][0]))
        try:
            mo = dicom_date_re.search(do.StudyDate)
            self.interview_date = '%s/%s/%s' % (month, day, year)
        except:
            self.interview_date = None
        try:
            self.interview_age = do.PatientAge
        except:
            self.interview_age = None
        try:
            self.gender = do.PatientSex
        except:
            self.gender = None
        try:
            self.image_modality = do.Modality
        except:
            self.image_modality = None
        try:
            self.scanner_manufacturer_pd = do.Manufacturer
        except:
            self.scanner_manufacturer_pd = None
        try:
            self.scanner_type_pd = do.ManufacturerModelName
        except:
            self.scanner_type_pd = None
        try:
            self.magnetic_field_strength = do.MagneticFieldStrength
        except:
            self.magnetic_field_strength = None
        try:
            self.mri_repetition_time_pd = str(float(do.RepetitionTime) / 1000.0)
        except:
            self.mri_repetition_time_pd = None
        try:
            self.mri_echo_time_pd = str(float(do.EchoTime) / 1000.0)
        except:
            self.mri_echo_time_pd = None
        try:
            self.flip_angle = do.FlipAngle
        except:
            self.flip_angle = None
        try:
            self.acquisition_matrix = do.AcquisitionMatrix
        except:
            self.acquisition_matrix = None
        try:
            self.patient_position = do.PatientPosition
        except:
            self.patient_position = None
        try:
            self.photomet_interpret = do.PhotometricInterpretation
        except:
            self.photomet_interpret = None
        try:
            self.receive_coil = do.ReceiveCoilName
        except:
            self.receive_coil = None
        try:
            self.transmit_coil = do.TransmitCoilName
        except:
            self.transmit_coil = None
        return

    def _unpack(self):

        os.mkdir('%s/unpacked' % self._tempdir)
        self.files = {'DICOM': [], 
                      'NIfTI-1': [], 
                      'MINC': [], 
                      'AFNI': [], 
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
            file_type = _get_file_type(self._temp_source)
            self.files[file_type].append(self._source_base)
            os.symlink(self._temp_source, self.path(self._source_base))

        # now match up .HEADs and .BRIKs
        heads = set([ name[:-5] for name in self.files['AFNI'] \
                                if name.endswith('.HEAD') ])
        briks = set([ name[:-5] for name in self.files['AFNI'] \
                                if name.endswith('.BRIK') ])
        pairs = heads.intersection(briks)
        self.files['AFNI'] = list(pairs)
        lone_heads = [ base+'.HEAD' for base in heads-pairs ]
        lone_briks = [ base+'.BRIK' for base in briks-pairs ]
        self.files['other'].extend(lone_heads)
        self.files['other'].extend(lone_briks)

        # sort the file names
        for l in self.files.itervalues():
            l.sort()

        return

    def __del__(self):
        if self._clean_on_del:
            self.clean()
        return

    def exists(self):
        """report whether the file or S3 object exists

        must be defined in subclasses
        """
        raise NotImplementedError()

    def clean(self):
        """Clean up temporary files."""
        shutil.rmtree(self._tempdir)
        self._tempdir = None
        self.files = None
        self.nifti_1 = None
        self.nifti_1_gz = None
        self.afni = None
        self.xcede = None
        self.nrrd = None
        self.thumbnail = None
        return

    def path(self, fname):
        """Return the full path to a single file."""
        return '%s/unpacked/%s' % (self._tempdir, fname)

class Image(_BaseImage):

    """image-from-file class"""

    def __init__(self, source, attrs=None, check_existence=False):
        _BaseImage.__init__(self)
        self.source = os.path.abspath(source)
        if check_existence:
            if not self.exists():
                raise IOError(errno.ENOENT, 
                              "No such file or directory: '%s'" % self.source)
        self.files = None
        self._set_attributes(attrs)
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
                 attrs=None, check_existence=False):
        _BaseImage.__init__(self)
        if 'boto' not in sys.modules:
            raise ImportError('boto S3 connection module not found')
        self.source = source
        self._s3_access_key = s3_access_key
        self._s3_secret_key = s3_secret_key
        if check_existence:
            if not self.exists():
                raise ObjectNotFoundError(source)
        self.files = None
        self._set_attributes(attrs)
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
