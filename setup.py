#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-stella",
    version="0.1.1",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_stella"],
    install_requires=[
        "singer-python>=5.9.0",
        "PyJWT==1.7.1",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-stella=tap_stella:main
    """,
    packages=["tap_stella"],
    package_data = {
        "schemas": ["tap_stella/schemas/*.json"]
    },
    include_package_data=True,
)
