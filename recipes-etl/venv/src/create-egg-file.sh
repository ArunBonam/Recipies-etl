#!/bin/bash

rm -rf recpiesetl.egg-info
rm -rf build
rm -rf dist
python setup.py install
python setup.py bdist_egg

