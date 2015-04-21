modulelist = ['applier', 'calcstats','cuiprogress', 'fileinfo', 'imageio', 'imagereader', 'imagewriter', 'inputcollection', 'parallel', 'pixelgrid', 'rat', 'ratapplier', 'readerinfo', 'rioserrors', 'riostests', 'vectorreader']

for module in modulelist:
    outText = '''{0}
=========
.. automodule:: rios.{0}
   :members:
   :undoc-members:

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
'''.format(module)
    outfile = open('rios_{}.rst'.format(module),'w')
    outfile.write(outText)
    outfile.close
