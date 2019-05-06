#!/usr/bin/env python
from setuptools import setup, find_packages
import re


def read_version():
    """https://stackoverflow.com/a/7071358/386279"""
    VERSIONFILE = "bgbb_airflow/_version.py"

    with open(VERSIONFILE, "rt") as fp:
        verstrline = fp.read()
    VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
    mo = re.search(VSRE, verstrline, re.M)
    if mo:
        verstr = mo.group(1)
    else:
        raise RuntimeError(
            "Unable to find version string in %s." % (VERSIONFILE,)
        )
    return verstr


verstr = read_version()

test_deps = ["pytest"]

extras = {"test": test_deps}

setup(
    name="bgbb_airflow",
    version=verstr,
    description="Scripts to run airflow jobs using bgbb_lib",
    author="W Chris Beard",
    url="https://github.com/wcbeard/bgbb_airflow.git",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    # TODO: pin versions?
    install_requires=[
        "bgbb==0.1.5",
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
