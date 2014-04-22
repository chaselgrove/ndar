.. See file COPYING distributed with ndar_unpack for copyright and license.

ndar_unpack will download and unpack imaging data from NDAR.

Examples:

::

    ndar_unpack -t thumbnail.png NDAR_INVOX992PCX_image03_1350375953058.zip

    ndar_unpack --aws-access-key-id $access_key \
                --aws-secret-access-key $secret_key \
                -v volume.nii.gz \
                s3://NDAR_Central/NDAR_INVCH423DPL_image03_1357865972017.zip

For more information, run ``ndar_unpack -h``.

Dependencies
============

ndar_unpack needs the following to run:

* boto_
* pydicom_

To actually do anything useful, ndar_unpack will also need:

* FreeSurfer_ for most operations
* FSL_ for thumbnail generation
* nifti_tool from niftilib_ for NIfTI header dumping
* nibabel_ with Minc2Image for MINC2 support
* SimpleITK_ for NRRD support

Run ``ndar_unpack -S`` to run a self-check and report what components are 
found and what components are missing.

.. _boto: https://github.com/boto/boto/
.. _pydicom: https://code.google.com/p/pydicom/
.. _FreeSurfer: http://surfer.nmr.mgh.harvard.edu/
.. _FSL: http://fsl.fmrib.ox.ac.uk/fsl/fslwiki/
.. _niftilib: http://niftilib.sourceforge.net/
.. _nibabel: http://nipy.org/nibabel
.. _SimpleITK: http://www.simpleitk.org/

NDAR
====

NDAR is the `National Database For Autism Research`_.  Access to NDAR data is governed by NDAR policies.

.. _National Database For Autism Research: http://ndar.nih.gov/
