#!/usr/bin/env python

from distutils.core import setup

setup(
    name='starbelly',
    version='0.0.1',
    description='Client library for Starbelly web crawler',
    author='Mark E. Haase',
    author_email='mehaase@gmail.com',
    packages=['starbelly', 'starbelly_proto'],
    package_dir={
        'starbelly': 'starbelly',
        'starbelly_proto': 'starbelly_proto'
    },
    install_requires=[
        'protobuf',
        'websockets',
    ]
)
