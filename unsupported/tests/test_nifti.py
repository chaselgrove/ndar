import os
import nose.tools
import ndar

def test_nifti_nifti():
    """image is already a NIfTI-1 file"""
    im = ndar.Image('test_data/06025B_mprage.nii.gz')
    assert im.nifti_1 == im.path(im.files['NIfTI-1'][0])

def test_nifti_unzipped_nifti():
    """image is already an uncompressed NIfTI-1 file"""
    im = ndar.Image('test_data/a.nii')
    assert im.nifti_1 == im.path(im.files['NIfTI-1'][0])

def test_nifti_nonnifti():
    """image is not a NIfTI-1 file"""
    im = ndar.Image('test_data/NDAR_INVZU049GXV_image03_1326225820791.zip')
    assert os.path.exists(im.nifti_1)

def test_nifti_nonvolume():
    """image is not a volume"""
    im = ndar.Image('test_data/10_425-02_li1_146.png')
    nose.tools.assert_raises(AttributeError, lambda: im.nifti_1)

def test_nifti_mcfail():
    """mri_convert failure (by way of a bad image)"""
    im = ndar.Image('test_data/bogus.mnc')
    nose.tools.assert_raises(AttributeError, lambda: im.nifti_1)

def test_nifti_nifti_gz():
    """image is a gzipped NIfTI-1 file"""
    im = ndar.Image('test_data/06025B_mprage.nii.gz')
    assert im.nifti_1_gz == im.path(im.files['NIfTI-1'][0])

def test_nifti_unzipped_nifti_gz():
    """image is an unzipped NIfTI-1 file"""
    im = ndar.Image('test_data/a.nii')
    assert im.nifti_1_gz != im.path(im.files['NIfTI-1'][0])
    assert im.nifti_1_gz.endswith('.nii.gz')
    assert os.path.exists(im.nifti_1_gz)

# eof
