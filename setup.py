#!/usr/bin/env python
from setuptools import setup, find_packages

test_deps = [
    # 'coverage==4.5.2',
    # 'pytest-cov==2.6.0',
    # 'pytest-timeout==1.3.3',
    # 'moto==1.3.6',
    # 'mock==2.0.0',
    "pytest",
    # 'flake8==3.6.0'
]

extras = {"test": test_deps}

setup(
    name="bgbb_airflow",
    version="0.1.0",
    description="Scripts to run airflow jobs using bgbb_lib",
    author="W Chris Beard",
    url="https://github.com/wcbeard/bgbb_airflow.git",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    # TODO: pin versions?
    install_requires=[
        "bgbb==0.1.4a1",
        "numba>=0.34",
        "click",
        "pyarrow",
        "pyspark",
        "pytest",
        "six",
    ],
    tests_require=test_deps,
    extras_require=extras,
    classifiers=["Programming Language :: Python :: 3.5"],
    license=["Apache 2.0", "MIT"],
)
