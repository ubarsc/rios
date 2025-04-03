=====================
Environment Variables
=====================

RIOS honours the following environment variables which can be used to override default behaviour globally:

+-------------------------------+---------------------------------------+----------------+-----------------------+
|Environment Variable           |Description                            | Default        |  ApplierControls name |
+===============================+=======================================+================+=======================+
|RIOS_DFLT_DRIVER               |The name of the default GDAL driver    |HFA             | drivername            |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_DRIVEROPTIONS        |Creation Options to be passed to GDAL. |COMPRESSED=TRUE | creationoptions       |
|                               |Can be 'None'. This is now deprecated, |IGNOREUTM=TRUE  |                       |
|                               |in favour of RIOS_DFLT_CREOPT_*        |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_CREOPT_<driver>      |Driver-specific creation options.      |From generic    |creationoptions should |
|                               |These are new, and intended to         |variable        |be None to use these   |
|                               |supercede the old generic default.     |                |                       |
|                               |Can be specified for any driver,       |                |                       |
|                               |but defaults are given as below        |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_CREOPT_HFA           | Default creation options for HFA      |COMPRESS=YES    |                       |
|                               |                                       |IGNOREUTM=TRUE  |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_CREOPT_GTiff         | Default creation options for GTiff    |TILED=YES       |                       |
|                               |                                       |INTERLEAVE=BAND |                       |
|                               |                                       |COMPRESS=LZW    |                       |
|                               |                                       |BIGTIFF=IF_SAFER|                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_FOOTPRINT            | 0 for intersection, 1 for union       | Intersection   | footprint             |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_BLOCKXSIZE           | Window X size                         | 256            | windowxsize           |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_BLOCKYSIZE           | Window Y size                         | 256            | windowysize           |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_OVERLAP              | Overlap between blocks                | 0              | overlap               |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_HISTOGRAM_IGNORE_RFC40    | If set, this will force writing of    | Not set        | Not in controls       |
|                               | histogram to ignore GDAL's RFC40      |                |                       |
|                               | capabilities. Mostly helpful when     |                |                       |
|                               | using HFA files, as RFC40 seems to    |                |                       |
|                               | have some problems with them in       |                |                       |
|                               | versions of GDAL older than 2.2.0.    |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_OVERVIEWLEVELS       | Global default overview levels.       | 4,8,16,32,64,  | overviewLevels        |
|                               | A comma-separated list of reduction   | 128,256,512    |                       |
|                               | factors, as per gdaladdo command      |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_MINOVERLEVELDIM      | Minimum dimension of overview layers. | 128            | overviewMinDim        |
|                               | Overview layers with any dimension    |                |                       |
|                               | less than this will not be created.   |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_OVERVIEWAGGTYPE      | Default aggregation type for          | NEAREST        | overviewAggType       |
|                               | overviews, used with formats not      |                |                       |
|                               | supporting LAYER_TYPE                 |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_DFLT_AUTOCOLORTABLETYPE   | Enforces automatic color tables       | No automatic   | autoColorTableType    |
|                               | on thematic output rasters. Value is  | color table    |                       |
|                               | a string passed as autoColorTableType | generated      |                       |
|                               | to :func:`rios.rat.genColorTable()`   |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_NO_VRT_FOR_RESAMPLING     | If set to '1' will mean that          | 0 = Use VRT    | Not in controls       |
|                               | resampling with VRTs is disabled and  |                |                       |
|                               | a temporary file in the output format |                |                       |
|                               | will be used instead. Added in RIOS   |                |                       |
|                               | 1.4.5.                                |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_AWSBATCH_STACK            | The CloudFormation stack name to use  | RIOS           | Not in controls       |
|                               | for AWS Batch jobs                    |                |                       | 
+-------------------------------+---------------------------------------+----------------+-----------------------+
|RIOS_AWSBATCH_REGION           | The AWS Region to look for the        | ap-southeast-2 | Not in controls       |
|                               | CloudFormation stack specified by     |                |                       |
|                               | DFLT_BATCH_STACK.                     |                |                       |
+-------------------------------+---------------------------------------+----------------+-----------------------+
