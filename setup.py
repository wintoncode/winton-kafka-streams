#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
setup for Winton Kafka Streams package

"""

from setuptools import setup

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'javaproperties',
    'confluent-kafka>=0.11.4',
    'requests',
    'avro-python3'
]

test_requirements = [
    'pytest'
]

setup(
    name='Winton Kafka Streams',
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    description="Apache Kafka's Streams API for Python",
    long_description=readme,
    author="Winton Group",
    author_email='opensource@winton.com',
    url='https://github.com/wintoncode/winton_kafka_streams',
    packages=[
        'winton_kafka_streams',
    ],
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=True,
    keywords='streams kafka winton',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    extras_require={
        'develop': ['pytest', 'sphinx_rtd_theme'],
        'binning_example': ['jupyter', 'pandas', 'bokeh'],
    }
)
