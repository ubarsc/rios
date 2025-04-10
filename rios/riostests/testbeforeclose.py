"""
Test the beforeCloseFunc callback.

This is a function which is called just before the output file is closed,
and is passed the still-open Dataset.

"""
from osgeo import gdal
from rios import applier

from rios.riostests import riostestutils

TESTNAME = "TESTBEFORECLOSE"
FIXED_NAME = "fixedName"
FIXED_VAL = "fixedVal"


def run():
    """
    Run the test
    """
    riostestutils.reportStart(TESTNAME)
    
    ramp = 'ramp.img'
    riostestutils.genRampImageFile(ramp)
    outfile1 = 'out1.img'
    outfile2 = 'out2.img'

    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    controls = applier.ApplierControls()
    infiles.img = ramp
    outfiles.outimg1 = outfile1
    outfiles.outimg2 = outfile2
    nameValPair = ('dummyName', 'dummyVal')
    controls.setCallBeforeClose(beforeCloseWithArgs, nameValPair,
        imagename='outimg1')
    controls.setCallBeforeClose(beforeCloseNoArgs, imagename='outimg2')
    
    applier.apply(userFunc, infiles, outfiles, controls=controls)

    allOK = True

    # Check that the function ran OK
    ds = gdal.Open(outfile1)
    val = ds.GetMetadataItem(nameValPair[0])
    del ds
    ok = (val == nameValPair[1])
    if not ok:
        msg = "Failed in call to 'beforeClose' function (with args)"
        riostestutils.report(TESTNAME, msg)
    allOK = allOK and ok

    # Now check the one with no args
    ds = gdal.Open(outfile2)
    val = ds.GetMetadataItem(FIXED_NAME)
    del ds
    ok = (val == FIXED_VAL)
    if not ok:
        msg = "Failed in call to 'beforeClose' function (without args)"
        riostestutils.report(TESTNAME, msg)
    allOK = allOK and ok
    
    # Clean up
    for filename in [ramp, outfile1, outfile2]:
        riostestutils.removeRasterFile(filename)

    if ok:
        riostestutils.report(TESTNAME, "Passed")
    
    return ok


def userFunc(info, inputs, outputs):
    outputs.outimg1 = inputs.img
    outputs.outimg2 = inputs.img


def beforeCloseWithArgs(ds, name, val):
    """
    Called before closing. Sets an arbitrary metadata item on the ds.
    """
    ds.SetMetadataItem(name, val)


def beforeCloseNoArgs(ds):
    """
    Called before closing, but takes no arguments other than ds
    """
    ds.SetMetadataItem(FIXED_NAME, FIXED_VAL)


if __name__ == "__main__":
    run()
