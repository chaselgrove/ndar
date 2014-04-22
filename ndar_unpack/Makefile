default : build

build : dist/ndar_unpack-0.1.0.tar.gz

dist/ndar_unpack-0.1.0.tar.gz : 
	python setup.py sdist

register : 
	python setup.py register

upload : 
	python setup.py sdist upload

check : 
	python setup.py check

clean : 
	rm -f MANIFEST

clobber : clean
	rm -rf dist

# eof
