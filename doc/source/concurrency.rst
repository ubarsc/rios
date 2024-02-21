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

It is strongly recommended that a new program be largely debugged with
no concurrency, and that developers only switch on the concurrency after 
they are already confident it works well without.

The routines for processing raster attribute tables (rios.ratapplier) are
unaffected by any of the above, and still work entirely sequentially.

Timing
------
Effective use of concurrency relies on understanding how time is spent within 
the application. The RIOS apply function has some internal monitoring to assist
with this. The apply() function returns an object with a field called timings.
This timings object can generate a simple report on where time is being spent
during the run. ::

    rtn = apply(userFunc, infiles, outfiles)
    timings = rtn.timings
    reportStr = timings.formatReport()
    print(reportStr)

This will show a simple report like the following ::

    Wall clock elapsed time: 11.8 seconds

    Timer                Total (sec)  
    -------------------------------
    reading                6.6
    userfunction          33.7
    writing                4.2
    add_inbuffer           2.3
    pop_inbuffer           1.3
    add_outbuffer          0.0
    pop_outbuffer          7.6

This example was run with 4 compute workers and 1 read worker. The total amount
of time spent in each category is added up across threads, so will be larger
than the elapsed wall clock time shown at the top. 

The time spent waiting for the various buffers can provide important clues.
If a lot of time is being spent waiting to add to the input buffer, this may 
mean there are not enough compute workers taking blocks out. Similarly, a lot of
time spent waiting to pop blocks out of the input buffer may indicate that
adding some read workers would help. All of this depends on the hardware 
configuration, of course. Adding more compute workers on a single core CPU
will not usually help at all. 

Time spent waiting to add to the output buffer probably indicates too many 
compute workers, filling up the buffer faster than the writing thread can 
empty it.

The details will vary a lot with the application and the hardware available,
but in general this timing report will assist in deciding the most useful
parameters for the ConcurrencyStyle.

Deprecated Code
---------------
As part of this new (version 2.0) update to the internals of RIOS, some
sections of code were completely redesigned. The main interface to RIOS,
via the applier.apply() function, is entirely unchanged, and should not 
require any action from the user, and existing code should work exactly 
as before. This will not be changed in the future. 

However, some of the internal code is now obsolete, and is likely to be
removed at some date in the future. The main sections which are likely to be
affected are

* The entire ImageReader class
* The entire ImageWriter class
* The entire InputCollection class
* The entire VectorReader class
* The old parallel computation code within rios.parallel. This was never very
  efficient, and is now not used. Existing applications which use it 
  should update to the new concurrency style. Until then, they will still run,
  but internall the new style is used to emulate the old, with guesses at
  appropriate parameters. 

Any application code which makes direct use of these classes should be reviewed
with this in mind.
