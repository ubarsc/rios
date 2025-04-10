"""
Test the beforeCloseFunc callback.

This is a function which is called just before the output file is closed,
and is passed the still-open Dataset.

"""
from osgeo import gdal
from rios import applier

from rios.riostests import riostestutils

TESTNAME = "TESTBEFORECLOSE"


def run():
    """
    Run the test
    """
    riostestutils.reportStart(TESTNAME)
    
    ramp = 'ramp.img'
    riostestutils.genRampImageFile(ramp)
    outfile = 'out.img'

    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    controls = applier.ApplierControls()
    infiles.img = ramp
    outfiles.outimg = outfile
    nameValPair = ('dummyName', 'dummyVal')
    controls.setCallBeforeClose(callBeforeCloseFunc, nameValPair,
        imagename='outimg')
    
    applier.apply(userFunc, infiles, outfiles, controls=controls)

    # Check that the function ran OK
    ds = gdal.Open(outfile)
    val = ds.GetMetadataItem(nameValPair[0])
    del ds
    ok = (val == nameValPair[1])
    if not ok:
        msg = "Failed in call to 'beforeClose' function"
        riostestutils.report(TESTNAME, msg)
    
    # Clean up
    for filename in [ramp, outfile]:
        riostestutils.removeRasterFile(filename)

    if ok:
        riostestutils.report(TESTNAME, "Passed")
    
    return ok


def userFunc(info, inputs, outputs):
    outputs.outimg = inputs.img


def callBeforeCloseFunc(ds, name, val):
    """
    Called before closing. Sets an arbitrary metadata item on the ds.
    """
    ds.SetMetadataItem(name, val)


if __name__ == "__main__":
    run()
