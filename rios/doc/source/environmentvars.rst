=====================
Environment Variables
=====================

RIOS honours the following environment variables which can be used to override default behaviour globally:

+-------------------------------+---------------------------------------+----------------+-----------------------+
|Environment Variable           | Description                           | Default        |  ApplierControls name |
+===============================+=======================================+================+=======================+
|RIOS_DFLT_DRIVER               | The name of the default GDAL driver   | HFA            | drivername            |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_DRIVEROPTIONS        | Creation Options to be passed to GDAL.| COMPRESSED=TRUE| creationoptions       |
|                               | Can be 'None'.                        | IGNOREUTM=TRUE |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_FOOTPRINT            | 0 for intersection, 1 for union       | Intersection   | footprint             |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_BLOCKXSIZE           | Window X size                         | 200            | windowxsize           |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_BLOCKYSIZE           | Window Y size                         | 200            | windowysize           |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_OVERLAP              | Overlap between blocks                | 0              | overlap               |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_HISTOGRAM_IGNORE_RFC40    | If set, this will force writing of    | Not set        | Not in controls       |
|                               | histogram to ignore GDAL's RFC40      |                |                       |
|                               | capabilities. Mostly helpful when     |                |                       |
|                               | using HFA files, as RFC40 seems to    |                |                       |
|                               | have some problems with them          |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_JOBMGRTYPE           | See :mod:`rios.parallel.jobmanager`   |                |                       |
|RIOS_PBSJOBMGR_QSUBOPTIONS     | for a description of these variables  |                |                       |
|RIOS_PBSJOBMGR_INITCMDS        |                                       |                |                       |
|RIOS_SLURMJOBMGR_SBATCHOPTIONS |                                       |                |                       |
|RIOS_SLURMJOBMGR_INITCMDS      |                                       |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_OVERVIEWLEVELS       | Global default overview levels.       | 4,8,16,32,64,  | overviewLevels        |
|                               | A comma-separated list of reduction   | 128,256,512    |                       |
|                               | factors, as per gdaladdo command      |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_MINOVERLEVELDIM      | Minimum dimension of overview layers. | 33             | overviewMinDim        |
|                               | Overview layers with any dimension    |                |                       |
|                               | less than this will not be created.   |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_OVERVIEWAGGTYPE      | Default aggregation type for          | AVERAGE        | overviewAggType       |
|                               | overviews, used with formats not      |                |                       |
|                               | supporting LAYER_TYPE                 |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
