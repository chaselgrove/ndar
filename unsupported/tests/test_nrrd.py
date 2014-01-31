import os
import nose.tools
import ndar

def test_nrrd_nonvolume():
    """image is not a volume"""
    im = ndar.Image('test_data/10_425-02_li1_146.png')
    nose.tools.assert_raises(AttributeError, lambda: im.nrrd)

def test_nrrd_nrrd():
    """image is already a NRRD file"""
    im = ndar.Image('test_data/002-12AU_t1w.nrrd')
    assert im.nrrd == im.path(im.files['NRRD'][0])

def test_nrrd_dicom():
    """image is DICOM"""
    im = ndar.Image('test_data/s1615890.zip')
    assert os.path.exists(im.nrrd)

# eof
