import nose.tools
import os
import shutil
import ndar

def test_bad_file():
    nose.tools.assert_raises(Exception, 
                             lambda: ndar.Image('test_data/bogus', 
                                                check_existence=True))
    i = ndar.Image('test_data/bogus')
    assert not i.exists()
    nose.tools.assert_raises(Exception, lambda: i.files)

def test_tempdir_remove():
    i = ndar.Image('test_data/06025B_mprage.nii.gz')
    tempdir = i._tempdir
    del i
    assert not os.path.exists(tempdir)

def test_tempdir_noremove():
    i = ndar.Image('test_data/06025B_mprage.nii.gz')
    tempdir = i._tempdir
    i._clean = False
    del i
    assert os.path.exists(tempdir)
    shutil.rmtree(tempdir)

def test_nifti_1():
    i = ndar.Image('test_data/06025B_mprage.nii.gz')
    assert i.files['NIfTI-1'] == ['06025B_mprage.nii.gz']
    for (k, v) in i.files.iteritems():
        if k != 'NIfTI-1':
            assert not v
    assert os.path.exists(i.path('06025B_mprage.nii.gz'))

def test_dicom():
    i = ndar.Image('test_data/NDAR_INVXT425UFT_image03_1357865972017.zip')
    assert i.files['DICOM'] == ['0001.dcm']
    for (k, v) in i.files.iteritems():
        if k != 'DICOM':
            assert not v
    assert os.path.exists(i.path('0001.dcm'))

def test_brik():
    i = ndar.Image('test_data/NDAR_INVZU049GXV_image03_1326225820791.zip')
    assert i.files['AFNI'] == ['20783.spgr_at+tlrc']
    for (k, v) in i.files.iteritems():
        if k != 'AFNI':
            assert not v
    assert os.path.exists(i.path(i.files['AFNI'][0] + '.HEAD'))
    assert os.path.exists(i.path(i.files['AFNI'][0] + '.BRIK'))
    assert not os.path.exists(i.path(i.files['AFNI'][0]))

def test_dicom_2():
    i = ndar.Image('test_data/s1615890.zip')
    assert len(i.files['DICOM']) == 166
    for (k, v) in i.files.iteritems():
        if k != 'DICOM':
            assert not v
    assert os.path.exists(i.path('i1616037.MRDC.160'))

def test_s3_bad_keys():
    path = 's3://NDAR_Central/submission_9709/T0177-1-1/NDARBF372RNH-DWI.nrrd'
    ak = 'bogus'
    sk = 'bogus'
    nose.tools.assert_raises(Exception, 
                             lambda: ndar.S3Image(path, ak, sk, 
                                                  check_existence=True))
    i = ndar.S3Image(path, ak, sk)
    nose.tools.assert_raises(Exception, lambda: i.files)

def test_s3_bad_bucket():
    path = 's3://bogus_bucket/submission_9709/T0177-1-1/NDARBF372RNH-DWI.nrrd'
    ak = os.environ['S3ACCESS']
    sk = os.environ['S3SECRET']
    nose.tools.assert_raises(Exception, 
                             lambda: ndar.S3Image(path, ak, sk, 
                                                  check_existence=True))
    i = ndar.S3Image(path, ak, sk)
    assert not i.exists()
    nose.tools.assert_raises(Exception, lambda: i.files)

def test_s3_bad_object():
    path = 's3://NDAR_Central/bogus_object'
    ak = os.environ['S3ACCESS']
    sk = os.environ['S3SECRET']
    nose.tools.assert_raises(Exception, 
                             lambda: ndar.S3Image(path, ak, sk, 
                                                  check_existence=True))
    i = ndar.S3Image(path, ak, sk)
    assert not i.exists()
    nose.tools.assert_raises(Exception, lambda: i.files)

def test_s3_nifti_1():
    path = 's3://NDAR_Central/submission_9575/00365B_mprage.nii.gz'
    ak = os.environ['S3ACCESS']
    sk = os.environ['S3SECRET']
    i = ndar.S3Image(path, ak, sk)
    assert i.files['NIfTI-1'] == ['00365B_mprage.nii.gz']
    for (k, v) in i.files.iteritems():
        if k != 'NIfTI-1':
            assert not v
    assert os.path.exists(i.path('00365B_mprage.nii.gz'))

def test_s3_zip():
    path = 's3://NDAR_Central/submission_9275/NDARTW376PB4_FSPGR.zip'
    ak = os.environ['S3ACCESS']
    sk = os.environ['S3SECRET']
    i = ndar.S3Image(path, ak, sk)
    assert len(i.files['DICOM']) == 328
    for (k, v) in i.files.iteritems():
        if k != 'DICOM':
            assert not v
    assert os.path.exists(i.path(i.files['DICOM'][0]))

# eof
