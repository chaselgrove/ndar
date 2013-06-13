NDAR
====

Module for working with NDAR data.

Classes
-------

### Image: image03 file class.

Image03 files are typically ZIP files containing image volumes or 2-D images.  The Image class constructor takes the path to one of these files as an argument and provides convenience routines for access to the contained files.

Image03 constructors may be local files or S3 objects.  In the case of S3 objects, an AWS access key and AWS secret key must also be passed.

Attributes:

* source: the file source (as passed to the constructor).
* file_dict: a dictionary of lists contained file names, keyed by file type.  Types are:
    * BRIK
    * DICOM
    * JPEG
    * MINC
    * NIfTI-1
    * NRRD
    * PNG
    * PNG
    * other

Because BRIK files are paired .HEAD and .BRIK files, the list of files in file_dict['BRIK'] are the base file names, to which '.HEAD' and '.BRIK' should be appended.  .BRIKs without .HEADs and .HEADs without .BRIKs are listed in file_dict['other'].

Methods:

* close(): Remove the temporary directory containg downloaded and unpacked files.
* path(fname): Return the full path to a contained file.

Examples:

    >>> im = ndar.Image03('mprage.nii.gz')
    >>> im.file_dict['NIfTI-1']
    ['mprage.nii.gz']

    >>> im = ndar.Image03('NDARTW376PB4_FSPGR.zip')
    >>> len(im.file_dict['DICOM'])
    328

    >>> im = ndar.Image('s3://NDAR_Central/submission_9275/NDARTW376PB4_FSPGR.zip', 
                        s3_access_key, 
                        s3_secret_key)
    >>> for fname in im.file_dict['DICOM']:
    ...     full_path = im.path(fname)
    ...     shutil.copy(full_path, destination_directory)

    >>> im = ndar.Image('NDAR_INVZU049GXV_image03_1326225820791.zip')
    >>> im.file_dict['BRIK']
    ['20783.spgr_at+tlrc']
    >>> os.path.exists(i.path('20783.spgr_at+tlrc'))
    False
    >>> os.path.exists(i.path('20783.spgr_at+tlrc.HEAD'))
    True
    >>> os.path.exists(i.path('20783.spgr_at+tlrc.BRIK'))
    True
