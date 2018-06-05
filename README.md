# Winton Kafka Streams

[![Build Status](https://travis-ci.org/wintoncode/winton-kafka-streams.svg?branch=master)](https://travis-ci.org/wintoncode/winton-kafka-streams)

Implementation of [Apache Kafka's Streams API](https://kafka.apache.org/documentation/streams/) in Python.

## What and why?
Apache Kafka is an open-source stream processing platform developed
by the Apache Software Foundation written in Scala and Java. Kafka
has Streams API added for building stream processing applications
using Apache Kafka. Applications built with Kafka's Streams API do not require any
setup beyond the provision of a Kafka cluster.

Winton Kafka Streams is a Python implementation of Apache Kafka's
Streams API. It builds on Confluent's librdkafka (a high
performance C library implementing the Kafka protocol) and the
Confluent Python Kafka library to achieve this.

The power and simplicity of both Python and Kafka's Streams API combined
opens the streaming model to many more people and applications.

## Getting started

### Dependencies

The minimum Python version is currently 3.6 and a working Kafka
cluster (a single replica is sufficient for testing) are required.

You will require [librdkafka](https://github.com/edenhill/librdkafka). On Mac OS, we recommend installing this via HomeBrew and setting `CFLAGS=-I/usr/local/include` and `LDFLAGS=-L/usr/local/lib` is
when installing Confluent Python Kafka (see below).
The librdkafka GitHub page lists packages available for Debian and Ubuntu, as well as RPMS.
For Arch Linux it is available via [AUR](https://aur.archlinux.org/packages/librdkafka-git/).

Confluent Python Kafka is also required and it should be installed
as a dependency by pip.

### Installing

Cloning the Winton Kafka Streams repository from GitHub is
recommended if you want to contribute to the project.
Then use
`pip install --editable <path/to/winton_kafka_streams>[develop]`
to install as an editable workspace with additional dependencies
required for development.
You may need to do this using `sudo` on Linux.

If you want to install the code and get a feel for it as a user then
we recommend using `pip install git+https://github.com/wintoncode/winton-kafka-streams`.

### Running tests
Tests will run when py.test is called in the root of the repository.

### Running examples
To run examples, you must have cloned the code locally from GitHub.

The debug and wordcount examples will run without further additional
requirements.

The Jupyter notebook in the binning example requires some additional
packages. Install these with the command:

pip install <path/to/winton_kafka_streams>[binning_example]

## Contributing
Please see the CONTRIBUTING.md document for more details on getting involved.

## Contact
 - GitHub: https://github.com/wintoncode/
 - Email: opensource@winton.com
 - Twitter: @wintoncapital
