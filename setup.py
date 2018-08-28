#!/usr/bin/env python
import sys
from pathlib import Path
from setuptools import setup, find_packages

if not sys.version_info >= (3, 6):
    raise RuntimeError('Tinsel requires at least python 3.6')

readme = Path('README.rst').read_text()
history = Path('HISTORY.rst').read_text()

requirements = ['pyspark>=2.3', ]
setup_requirements = ['pytest-runner', ]
test_requirements = ['pytest', ]

setup(
    author="Andriy Kushnir (Orhideous)",
    author_email='me@orhideous.name',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="PySpark schema generator",
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='tinsel',
    name='tinsel',
    packages=find_packages(include=['tinsel']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/Orhideous/tinsel',
    version='0.2.0',
    zip_safe=False,
)
