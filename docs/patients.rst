Patients
========

A patient stay can be searched by IPP (``lifetimeid``) and by NDA, the hospital
stay number (``encounterid``). Both are returned as columns:

.. code-block:: python

   from dwclib import read_patients

   stays = read_patients(ipp="9000000017")
   stays = read_patients(nda="70001")
   stays[["lifetimeid", "encounterid", "bedlabel", "data_begin", "data_end"]]

.. automodule:: dwclib.patients
   :members:
