# Winton Kafka Streams

Implementation of [Apache Kafka Streams](https://kafka.apache.org/documentation/streams/) in Python. 

## What and why?
Apache Kafka is an open-source stream processing platform developed
by the Apache Software Foundation written in Scala and Java. Kafka
Streams is a Java library for building distributed stream processing
apps using Apache Kafka. 

Winton Kafka Streams is a Python implementation of the Apache Kafka
Streams project. It builds on Confluent's librdkafka (a high
performance C library implementing the Kafka protocol) and the
Confuent Python Kafka library to achieve this. 

## Getting started

### Installing
The minimum Python version is currently 3.6 and a working Kafka
cluster (a single replica is sufficient for testing) are required.

Confluent Python Kafka is also required and it should install
as a dependency by pip. If it fails during installation, 
then we recommend installing librdkafka with HomeBrew and setting 
`CFLAGS=-I/usr/local/include` and `LDFLAGS=-L/usr/local/lib` is
recommended. 

Cloning the Winton Kafka Streams repository from GitHub is
recommended if you want to contribute to the project. Use: 
`pip install --editable <path/to/winton_kafka_streams>`
to install as an editable development workspace. 

If you want to install the code and get a feel for it as a user then
we recommend using pip install which will pull the package from PyPI
or your local mirror. 

### Running tests
You will need to install these dependencies manuall into your environment:
 - pytest
 
We chose not to add these dependencies to the pip requirements as they
are required for development only and not for running the package. 

Tests will run when py.test is called inside the root of the repository.

## Contributing
Please see the CONTRIBUTING.md document for more details on getting involved. 

## Contact
 - GitHub: https://github.com/wintoncode/
 - Email: opensource@winton.com
 - Twitter: @wintoncapital
