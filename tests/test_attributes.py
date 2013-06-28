import os
import nose.tools
import ndar

def test_no_attributes():
    im = ndar.Image('test_data/s1615890.zip')
    nose.tools.assert_raises(AttributeError, lambda: im.acquisition_matrix)

def test_dict_attributes():
    im = ndar.Image('test_data/s1615890.zip', {'acquisition_matrix': 'am'})
    assert im.acquisition_matrix == 'am'

def test_extracted_attributes_dicom():
    im = ndar.Image('test_data/s1615890.zip', ndar.EXTRACT)
    assert im.image_file_format == 'DICOM'

def test_extracted_attributes_nifti():
    im = ndar.Image('test_data/06025B_mprage.nii.gz', ndar.EXTRACT)
    assert im.image_file_format == 'NIFTI'

def test_extracted_attributes_png():
    im = ndar.Image('test_data/10_425-02_li1_146.png', ndar.EXTRACT)
    assert im.image_file_format == 'PNG'

def test_extracted_attributes_brik():
    im = ndar.Image('test_data/NDAR_INVZU049GXV_image03_1326225820791.zip', 
                    ndar.EXTRACT)
    assert im.image_file_format == 'AFNI'

# eof
