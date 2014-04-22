#!/usr/bin/python

from distutils.core import setup

long_description = open('README.rst').read()

setup(name='ndar-backend', 
      version='0.1.0', 
      description='NDAR back-end tools', 
      author='Christian Haselgrove', 
      author_email='christian.haselgrove@umassmed.edu', 
      url='https://github.com/chaselgrove/ndar/ndar_backend', 
      scripts=['store_first_all_results', 
               'store_recon_all_results', 
               'store_structural_qa'], 
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
