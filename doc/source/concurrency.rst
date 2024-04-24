===========
Concurrency
===========

Overview
--------

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
buffer size limit.

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
to each of the output files. This runs concurrently with reading and
computation of later blocks.

The docstring for the :class:`rios.structures.ConcurrencyStyle` class
contains a more detailed explanation of how the various style parameters 
interact, and how to choose a good concurrency style for a particular 
problem and hardware configuration. The `Compute Worker Kinds`_ section
has deeper discussion on appropriate use of each compute worker kind.

It is strongly recommended that a new program be largely debugged with
no concurrency, and that developers only switch on the concurrency after 
they are already confident it works well without.

The routines for processing raster attribute tables (rios.ratapplier) are
unaffected by any of the above, and still work entirely sequentially.

Quick Start
-----------
This section is for those who don't really want to read all the documentation,
but just want to start using concurrency, without understanding it all. Not
a good idea, but here we go anyway.

We shall assume that you are running on a computer with multiple cores, and
for simplicity we will only consider the CW_THREADS kind of compute worker.
There are others, but they require more understanding.

  #. Make sure your program runs successfully with no concurrency. Use the
     return value of the ``applier.apply()`` function to find the timings::

       rtn = applier.apply(userFunc, infiles, outfiles)
       print(rtn.timings.formatReport())

     In particular, note the ``reading`` and ``userfunction`` times.

  #. Find the ratio between the ``reading`` and ``userfunction`` times. This
     tells us whether the program is dominated by I/O and computation. For many
     simpler raster processing tasks, the dominant part is reading the input
     files.
  #. If the ``reading`` time is substantially larger than the ``userfunction``
     time, the best thing is to start with some read workers. For example::

       controls = applier.ApplierControls()
       conc = applier.ConcurrencyStyle(numReadWorkers=2)
       controls.setConcurrencyStyle(conc)
       rtn = applier.apply(userFunc, infiles, outfiles, controls=controls)
       print(rtn.timings.formatReport())

     Note that the total time on each timer is added across all threads, and
     may well be larger than the wall clock elapsed time.

     Some points to note about read workers. Each runs as a separate thread,
     but may spend time waiting for the device, and so may not need much
     CPU time. If the files are compressed, then more CPU time is needed
     for the uncompression step, in each thread. If input files are being
     reprojected, that also adds to the CPU time required in each thread.
     So, the benefit of read workers and/or CPUS will vary depending on
     exactly what is going on.

  #. Vary the number of read workers to reduce the total elapsed wall time.
     If you are able to reduce this so much that the userfunction time becomes
     a substantial part of it, then you may benefit from some compute workers::

       conc = applier.ConcurrencyStyle(
                numReadWorkers=2,
                numComputeWorkers=2,
                computeWorkerKind=applier.CW_THREADS)

  #. Vary the number of read workers and compute workers to minimize the
     total elapsed wall time.

Note that the results of these experiments are specific to the particular
user function you are computing, the sizes and types of files, the device
that the files are stored on, and the computer you are running on. While past
experiences will be a useful guide (as always), you may need to repeat
this kind of experimentation when any of those factors changes.

If you wish to make use of other kinds of compute workers, things do get more
complicated. It is strongly recommended that you read the documentation
thoroughly, paying particular attention to the other parameters of the
ConcurrencyStyle() constructor.

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

This will show a simple report like the following::

    Wall clock elapsed time: 10.6 seconds

    Timer                Total (sec)
    -------------------------------
    reading                6.4
    userfunction          34.1
    writing                1.3
    closing                1.8
    add_inbuffer           2.3
    pop_inbuffer           0.5
    add_outbuffer          0.0
    pop_outbuffer          7.5

This example was run with 4 compute workers and 1 read worker. The total amount
of time spent in each category is added up across threads, so will be larger
than the elapsed wall clock time shown at the top.

For comparison, when run with no concurrency, the same task has the following
timings::

    Wall clock elapsed time: 35.1 seconds

    Timer                Total (sec)
    -------------------------------
    reading                4.4
    userfunction          27.7
    writing                1.2
    closing                1.7

The time spent waiting for the various buffers can provide important clues.
If a lot of time is being spent waiting to add to the input buffer, this may 
mean there are not enough compute workers taking blocks out. Similarly, a lot of
time spent waiting to pop blocks out of the input buffer may indicate that
adding some read workers might help. All of this depends on the hardware
configuration, of course. Adding more compute workers on a single core CPU
will not usually help at all. 

Time spent waiting to add to the output buffer probably indicates too many 
compute workers, filling up the buffer faster than the writing thread can 
empty it.

The details will vary a lot with the application and the hardware available,
but in general this timing report will assist in deciding the most useful
parameters for the ConcurrencyStyle.

Compute Worker Kinds
--------------------
This section describes the details of each of the different kinds of
compute worker.

**CW_THREADS**

Each compute worker will be a separate thread within the current process. They
are all running within the same Python interpreter, using 
concurrent.futures.ThreadPoolExecutor.

This is very efficient, and well suited when the program is running on a
multi-CPU machine, with few restrictions on how many threads a single 
program may use. Set the number of computeWorkers to be a little below the
number of CPUs (or CPU cores) available. Each compute worker does no reading
of its own, and just uses the block buffers to supply it with blocks of
data to compute with. The computeWorkersRead argument should be set to False.

Since all threads are within the same Python instance, if the user is doing
computation which does not release the Python GIL, then this may limit the
amount of parallel computation. Most operations with tools like numpy and 
scipy do release the GIL, and so it is not usually a problem. See CW_SUBPROC
as a possible alternative.

**CW_AWSBATCH**

Each compute worker runs as a separate AWS Batch job. Specific AWS infrastructure
needs to be available. See :doc:`awsbatch` for more information. Note that no
data is processed until all the AWS Batch sub jobs are running.

Communication between the batch jobs and the main thread is handled via a
network socket, which is managed by an extra thread running in the main process.
That last point means that the main script may run one more thread than you
expect. This network socket will use a port number in the range 30000-50000,
so the batch nodes should be configured to allow this.

**CW_PBS**

Each compute worker runs as a separate job on a PBS batch queue. This is one
way to make effective use of a large cluster which is only accessible through
a PBS queue, but it does have its limitations and complexities. If this
approach turns out to be too onerous for the PBS system available, the user
is advised to consider just running jobs which use CW_THREADS instead.

Using CW_PBS does assume that the batch cluster has relatively high
availability. If the main script starts running, but the worker jobs are too
slow to start as well, then the writer thread will timeout while waiting for
compute workers to supply it with data to write. Such a timeout is important
to have (otherwise failures would mean it may wait forever), but it does mean
that if the worker jobs are queued for too long, then using CW_PBS may not
be appropriate. If the writer timeout becomes a problem, it can be set to None
(computeBufferPopTimeout=None), in which case it will never timeout. Obviously
this should be used with caution.

Since PBS is generally used to manage a whole cluster, each compute worker may
be running on a separate machine. This makes it quite advantageous to have each
worker do its own reading, so one would usually run with
computeWorkersRead=True. However, in some situations, the batch nodes may be 
unable to read the input data directly (e.g. they may be on a private network 
with no direct access to the wider internet), in which case one would set 
computeWorkersRead=False.

Setting singleBlockComputeWorkers=True will generate a separate compute worker,
and thus a separate PBS job, for each block of processing (thus it ignores
numComputeWorkers, which is taken to be equal to the number of blocks to
process). This would be
a good option in a PBS cluster which prioritizes short jobs over long ones, as
the PBS scheduler would find it easy to allocate each of the individual jobs,
and so throughput would be quite high. However, it does imply a reasonable 
level of availability on the queue. The main originating thread will be waiting
for some output to come from the individual jobs, and if they get stuck
behind other jobs for too long, the main job will timeout.

Communication between the jobs and the main thread is handled via a network
socket, which is managed by an extra thread running in the main process. 
That last point means that the main script may run one more thread than you
expect. By default, the network address of this socket is given to the compute
worker jobs via a small file in a shared temporary directory. If no
shared temporary directory is available to the batch nodes, then
set sharedTempDir=False, and it will be passed on the command line for each
job. However, this is notionally less secure, since the command line is,
in principle, publicly visible, so this is not recommended.

The PBS jobs will honour two environment variables, which can be used to modify
their behaviour. These are

.. list-table::
   :widths: 20, 60
   :header-rows: 1

   * - Environment Variable
     - Description
   * - RIOS_PBSJOBMGR_QSUBOPTIONS
     - A single string of space-separated options to the qsub command. This
       will be embedded in the top of each job shell script with the ``#PBS``
       prefix.
   * - RIOS_PBSJOBMGR_INITCMDS
     - Any initial commands which should be executed at the start of the
       job shell script. The value is a single string which is inserted
       into the script.

**CW_SLURM**

This behaves exactly like the CW_PBS compute workers, but using the SLURM
batch queue system instead. See the PBS description.

It also honours two environment variables, very similarly to CW_PBS. Their
names are ``RIOS_SLURMJOBMGR_SBATCHOPTIONS`` and ``RIOS_SLURMJOBMGR_INITCMDS``.
See the corresponding PBS environment variables (above) for the corresponding
descriptions.

**CW_SUBPROC**

This was implemented mainly for testing, and is not intended for general
use.

Each compute worker runs as a separate process, started with subprocess.Popen,
and thus runs in its own Python interpreter. For this reason, it may be a
useful alternative to CW_THREADS, for tasks which do not release the GIL. 
However, apart from that, there is probably no good reason to use this, and
CW_THREADS is preferred.

Since all workers are on the same machine, there is no particular benefit to 
having each worker do its own reading, so this should be used with
computeWorkersRead=False.

Style Summary Table
-------------------
This table summarizes a few of the most common combinations of parameters
to the ConcurrencyStyle constructor. See above for more details, and also
in the docstring for :class:`rios.structures.ConcurrencyStyle`.

.. list-table::
   :widths: 30, 50
   :header-rows: 1

   * - Main Parameters
     - Description
   * - numReadWorkers=0 computeWorkerKind=CW_NONE numComputeWorkers=0
     - This is the default. No concurrency is enabled. There is a single loop
       over all blocks, and each iteration does read-compute-write in sequence
   * - numReadWorkers=n computeWorkerKind=CW_NONE numComputeWorkers=0
     - Creates *n* read worker threads, which feed data into the input block
       buffer. The main loop is as before, but the 'read' step just pops
       available blocks of data out of the buffer, and then does compute-write
       in the main thread. There is a total of *n+1* threads running. In an
       I/O bound task, this may well be sufficient.
   * - numReadWorkers=n computeWorkerKind=CW_THREADS numComputeWorkers=m
     - Creates *n* read worker threads and *m* compute worker threads, all
       within the current process. The read workers put data into the input
       buffer, the compute workers take data from there and put computed blocks
       into the output buffer. The main loop pops available blocks from the
       output buffer and writes them. There is a total of *n+m+1* threads
       running. In a compute-bound task, just 1 read worker may be enough.
   * - numReadWorkers=n computeWorkerKind=CW_AWSBATCH numComputeWorkers=m
       computeWorkersRead=True
     - Runs *m* batch jobs with a single compute worker thread each, on
       separate machines. Each compute worker has *n* read worker threads,
       plus the compute thread. The computed blocks are put into the output
       buffer. The main thread, on the originating machine, pops blocks out
       of the output buffer and writes them. It maintains 1 extra thread to
       manage the socket for communicating with worker machines. The
       originating process thus has 2 threads, while each of the *m* batch
       jobs has a single process with a total of *n+1* threads.

       This description also applies to CW_PBS and CW_SLURM.

Deprecated Code
---------------
As part of this new (version 2.0) update to the internals of RIOS, some
sections of code were completely redesigned. The main interface to RIOS,
via the applier.apply() function, is entirely unchanged, and should not 
require any action from the user, and existing code should work exactly 
as before. This will not be changed in the future. 

However, some of the internal code is now obsolete, and is likely to be
removed at some date in the future. The main sections affected are

* The entire ImageReader class
* The entire ImageWriter class
* The entire InputCollection class
* The entire VectorReader class
* The old parallel computation code within rios.parallel. This was never very
  efficient, and is now not used. Existing applications which use it 
  should update to the new concurrency style. Until then, they will still run,
  but internally the new style is used to emulate the old, with guesses at
  appropriate parameters. 

Any application code which makes direct use of these classes should be reviewed
with this in mind.
