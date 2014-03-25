from distutils.core import setup
from setuptools import find_packages

setup(
    name='busby',
    version='0.10',
    author="Jessie Frazelle",
    author_email="jessie@yhathq.com",
    url="https://github.com/yhat/busby",
    packages=find_packages(),
    description="Parse a csv file and send through a websocket.",
    license='BSD',
    long_description=open('README.rst').read(),
    scripts=['bin/busby'],
    install_requires=[
        "websocket-client==0.12.0",
        "docopt==0.6.1",
        "pandas==0.13.0"
    ]
)