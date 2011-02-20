#!/bin/sh

python setup.py sdist
cd dist/
rm -rf rios-1.0
tar xvfz rios-1.0.tar.gz
cd rios-1.0/
python setup.py install --prefix=/opt/osgeo/local/rios/1.0
cd ../..


