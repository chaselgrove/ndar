import os
import nose.tools
import ndar

def test_thumbnail():
    im = ndar.Image('test_data/06025B_mprage.nii.gz')
    assert os.path.exists(im.thumbnail)

def test_thumbnail_nonvoume():
    """image is not a volume"""
    im = ndar.Image('test_data/10_425-02_li1_146.png')
    nose.tools.assert_raises(AttributeError, lambda: im.thumbnail)

def test_thumbnail_slicerfail():
    """slicer failure (by way of a bad image)"""
    im = ndar.Image('test_data/bogus.nii.gz')
    nose.tools.assert_raises(AttributeError, lambda: im.thumbnail)

# eof
