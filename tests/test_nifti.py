import os
import nose.tools
import ndar

def test_nifti_nifti():
    """image is already a NIfTI file"""
    im = ndar.Image('test_data/06025B_mprage.nii.gz')
    assert im.nifti == im.path(im.files['NIfTI-1'][0])

def test_nifti_nonnifti():
    """image is not a NIfTI file"""
    im = ndar.Image('test_data/NDAR_INVZU049GXV_image03_1326225820791.zip')
    assert os.path.exists(im.nifti)

def test_nifti_nonvolume():
    """image is not a volume"""
    im = ndar.Image('test_data/10_425-02_li1_146.png')
    nose.tools.assert_raises(AttributeError, lambda: im.nifti)

def test_nifti_mcfail():
    """mri_convert failure (by way of a bad image)"""
    im = ndar.Image('test_data/bogus.mnc')
    nose.tools.assert_raises(AttributeError, lambda: im.nifti)

# eof
