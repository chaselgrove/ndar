import nose.tools
import os
import ndar

def test_package():
    package = ndar.Package('test_data/package')
    assert len(package.images) == 12

def test_noent_package():
    nose.tools.assert_raises(Exception, lambda: ndar.Package('test_data/bogus'))

def test_mysql_package():
    package = ndar.MySQLPackage(os.environ['MYSQL_HOST'], 
                                os.environ['MYSQL_USER'], 
                                os.environ['MYSQL_PASSWORD'], 
                                os.environ['MYSQL_DATABASE'])
    assert len(package.images) == 7626

# eof
