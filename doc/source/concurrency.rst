===========
Concurrency
===========

Beginning with version 2.0, RIOS now has strong capacity for concurrency
in both reading and computation. With suitable hardware configurations, 
reading of input files can be divided between a number of read worker threads,
and can read ahead of the simultaneous computation of results. 
Computation can be spread across multiple worker threads (or processes or 
batch jobs), and all of this can be simultaneous with writing of output files. 
The default behaviour has no concurrency, but options available through the
controls object can turn on some or all of these features, without
requiring changes to the computation in the user function.

The controls object now has a :func:`rios.applier.ApplierControls.setConcurrencyStyle` 
method, which takes a :class:`rios.structures.ConcurrencyStyle` object. 
The constructor for this class has a number of options (see the class
docstring for full details). 

RIOS processing falls into three stages.

    1. Reading blocks of data from the input files
    2. Computation, i.e. running the user function on complete
       blocks of input data, to produce blocks of output data
    3. Writing blocks of output data to the output files

By default, no concurrency is enabled. The three stages above take place
in sequence, for each block, before moving to the next block

If concurrency is enabled, the same model still operates, but parts of it can
be performed simultaneously, within the limits of the available hardware.

The reading of input data is divided up at the level of single blocks of data
from each input file. These are used to assemble complete input blocks, 
ready for the user function to operate on, which are placed in an input
block buffer. The reading threads can run ahead of computation, up to the 
limits of the buffer.

The computation is divided at the level of complete blocks of input data,
i.e. single blocks from all the input files together. Each compute worker
is running the user function on one complete input block at a time. The
compute workers can run concurrently, with details dependent on the
computeWorkerKind (see below). As each output block is completed, it is
added to the output block buffer (up to the buffer size), ready for the 
writing thread to write it to the output files.

There is only a single writing thread, and it writes individual blocks to 
all of the output files. It waits for completed blocks to become available
in the output block buffer, and as soon as they are ready, it writes the data
to each of the output files. this runs concurrent with reading and computation
on later blocks.

The docstring for the :class:`rios.structures.ConcurrencyStyle` class
contains a detailed explanation of how the various style parameters interact,
and how to choose a good concurrency style for a particular problem and
hardware configuration. 

