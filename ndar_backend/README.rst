.. See file COPYING distributed with ndar-backend for copyright and license.

Back-end tools for NDAR.

This package contains the following scripts:

* store_first_all_results
* store_recon_all_results
* store_structural_qa

The following environment variables must be defined for database uploads:

* DB_HOST
* DB_SERVICE
* DB_USER
* DB_PASSWORD

Dependencies
============

The upload scripts require cx_Oracle_ to run.

.. _cx_Oracle: http://cx-oracle.sourceforge.net/

NDAR
====

NDAR is the `National Database For Autism Research`_.

.. _National Database For Autism Research: http://ndar.nih.gov/
