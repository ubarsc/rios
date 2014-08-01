"""
This sub-package contains parts of the "cloud" module from the PiCloud project.
The PiCloud project has since been wound up, but the software was licensed under LGPL,
as shown in the component module files, and is still floating around the Internet
in various other projects. It is not widely used, which is rather a shame,
as they have done an awesome job with it. Its proper home is
    https://pypi.python.org/pypi/cloud/

I obtained a cut-down subset of it from this project
    http://code.google.com/p/playdoh/source/browse/trunk/playdoh/codehandler
on 2014-07-25. 
The only components I have here are those required to reliably serialize Python
objects and functions. I have installed this within RIOS so that I will always
have access to these components, because the PiCloud client is probably
not commonly installed on people's systems, and may well disappear at some stage
if no-one is maintaining it. I wa also reluctant to add another dependency to RIOS. 

When importing this into RIOS, I first try to import from the proper module, and 
only if that fails do I import this local copy. 

"""
