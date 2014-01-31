from distutils.core import setup
from setuptools import find_packages

setup(
    name='busby',
    version='0.1',
    packages=find_packages(),
    license='BSD',
    long_description=open('README.rst').read(),
    scripts=['bin/busby'],
    install_requires=[
        "websocket-client==0.12.0",
        "docopt==0.6.1"
    ]
)