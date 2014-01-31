import os
import nose.tools
import ndar

def test_clean():
    i = ndar.Image('test_data/06025B_mprage.nii.gz')
    tempdir = i._tempdir
    assert os.path.exists(tempdir)
    i.clean()
    assert not os.path.exists(tempdir)
    assert os.path.exists(i._tempdir)

def test_nifti_1():
    i = ndar.Image('test_data/06025B_mprage.nii.gz')
    nifti_1_file = i.path(i.files['NIfTI-1'][0])
    assert os.path.exists(nifti_1_file)
    i.clean()
    assert not os.path.exists(nifti_1_file)
    assert os.path.exists(i.path(i.files['NIfTI-1'][0]))

def test_dicom():
    i = ndar.Image('test_data/NDAR_INVXT425UFT_image03_1357865972017.zip')
    dicom_file = i.path(i.files['DICOM'][0])
    assert os.path.exists(dicom_file)
    i.clean()
    assert not os.path.exists(dicom_file)
    assert os.path.exists(i.path(i.files['DICOM'][0]))

def test_s3_nifti_1():
    path = 's3://NDAR_Central/submission_9575/00365B_mprage.nii.gz'
    ak = os.environ['S3ACCESS']
    sk = os.environ['S3SECRET']
    i = ndar.S3Image(path, ak, sk)
    nifti_1_file = i.path(i.files['NIfTI-1'][0])
    assert os.path.exists(nifti_1_file)
    i.clean()
    assert not os.path.exists(nifti_1_file)
    assert os.path.exists(i.path(i.files['NIfTI-1'][0]))

def test_s3_zip():
    path = 's3://NDAR_Central/submission_9275/NDARTW376PB4_FSPGR.zip'
    ak = os.environ['S3ACCESS']
    sk = os.environ['S3SECRET']
    i = ndar.S3Image(path, ak, sk)
    dicom_file = i.path(i.files['DICOM'][0])
    assert os.path.exists(dicom_file)
    i.clean()
    assert not os.path.exists(dicom_file)
    assert os.path.exists(i.path(i.files['DICOM'][0]))

def test_nifti_nifti():
    """image is already a NIfTI-1 file"""
    i = ndar.Image('test_data/06025B_mprage.nii.gz')
    nifti_1_file = i.nifti_1
    assert os.path.exists(nifti_1_file)
    i.clean()
    assert not os.path.exists(nifti_1_file)
    assert os.path.exists(i.nifti_1)

def test_nifti_nonnifti():
    """image is not a NIfTI-1 file"""
    i = ndar.Image('test_data/NDAR_INVZU049GXV_image03_1326225820791.zip')
    nifti_1_file = i.nifti_1
    assert os.path.exists(nifti_1_file)
    i.clean()
    assert not os.path.exists(nifti_1_file)
    assert os.path.exists(i.nifti_1)

def test_thumbnail():
    i = ndar.Image('test_data/06025B_mprage.nii.gz')
    thumbnail = i.thumbnail
    assert os.path.exists(thumbnail)
    i.clean()
    assert not os.path.exists(thumbnail)
    assert os.path.exists(i.thumbnail)

# eof
