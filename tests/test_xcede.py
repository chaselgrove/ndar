import os
import nose.tools
import ndar

def test_xcede_nonvolume():
    """image is not a volume"""
    im = ndar.Image('test_data/10_425-02_li1_146.png')
    nose.tools.assert_raises(AttributeError, lambda: im.xcede)

def test_xcede_fail():
    """conversion failure (by way of a bad image)"""
    im = ndar.Image('test_data/bogus.mnc')
    nose.tools.assert_raises(AttributeError, lambda: im.xcede)

def test_xcede_afni():
    """image is an AFNI file"""
    im = ndar.Image('test_data/NDAR_INVZU049GXV_image03_1326225820791.zip')
    assert os.path.exists(im.xcede)

def test_xcede_nifti():
    """image is a NIfTI-1 file"""
    im = ndar.Image('test_data/06025B_mprage.nii.gz')
    assert os.path.exists(im.xcede)

def test_xcede_minc():
    """image is a MINC file"""
    im = ndar.Image('test_data/a.mnc')
    assert os.path.exists(im.xcede)

def test_xcede_nrrd():
    """image is a NRRD file"""
    im = ndar.Image('test_data/002-12AU_t1w.nrrd')
    assert os.path.exists(im.xcede)

def test_xcede_dicom():
    """image is DICOM"""
    im = ndar.Image('test_data/s1615890.zip')
    assert os.path.exists(im.xcede)

# eof
