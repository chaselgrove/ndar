#!/usr/bin/python

from distutils.core import setup

long_description = open('README.rst').read()

setup(name='ndar_unpack', 
      version='0.1.0', 
      description='A script for checking and unpacking NDAR image data', 
      author='Christian Haselgrove', 
      author_email='christian.haselgrove@umassmed.edu', 
      url='https://github.com/chaselgrove/ndar/ndar_unpack', 
      scripts=['ndar_unpack'], 
      install_requires=['pydicom', 'boto'], 
      classifiers=['Development Status :: 3 - Alpha', 
                   'Environment :: Console', 
                   'Intended Audience :: Science/Research', 
                   'License :: OSI Approved :: BSD License', 
                   'Operating System :: OS Independent', 
                   'Programming Language :: Python', 
                   'Topic :: Scientific/Engineering'], 
      license='BSD license', 
      long_description=long_description
     )

# eof
