=====================
Environment Variables
=====================

RIOS honours the following environment variables which can be used to override default behaviour globally:

+---------------------------+---------------------------------------+----------------+-----------------------+
|Environment Variable       | Description                           | Default        |  ApplierControls name |
+===========================+=======================================+================+=======================+
|RIOS_DFLT_DRIVER           | The name of the default GDAL driver   | HFA            | drivername            |
+---------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_DRIVEROPTIONS    | Creation Options to be passed to GDAL.| COMPRESSED=TRUE| creationoptions       |
|                           | Can be 'None'.                        | IGNOREUTM=TRUE |                       |
+---------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_FOOTPRINT        | 0 for intersection, 1 for union       | Intersection   | footprint             |
+---------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_BLOCKXSIZE       | Window X size                         | 200            | windowxsize           |
+---------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_BLOCKYSIZE       | Window Y size                         | 200            | windowysize           |
+---------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_OVERLAP          | Overlap between blocks                | 0              | overlap               |
+---------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_HISTOGRAM_IGNORE_RFC40| If set, this will force writing of    | Not set        | Not in controls       |
|                           | histogram to ignore GDAL's RFC40      |                |                       |
|                           | capabilities. Mostly helpful when     |                |                       |
|                           | using HFA files, as RFC40 seems to    |                |                       |
|                           | have some problems with them          |                |                       |
+---------------------------+---------------------------------------+----------------+-----------------------+
