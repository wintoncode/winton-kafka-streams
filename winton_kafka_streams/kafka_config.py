"""
Configuration values that may be set to control behaviour of Winton Kafka Streams

Configuration may either be set inline in your application using:

import kafka_config
kafka_config.BOOTSTRAP_SERVERS = 'localhost:9092'

or as a file in java properties format. The property names are identical to
those used in the Java implementation for ease of sharing between both.

External files can be loaded using:

import kafka_config
kafka_config.read_local_config('path/to/kafka.streams.config')


"""

import logging
import os
import sys

import javaproperties
from typing import List

from .processor.serialization.serdes import BytesSerde, serde_as_string
from .errors.kafka_streams_error import KafkaStreamsError

log = logging.getLogger(__name__)

#### - Required options - ####

"""
A list of host/port pairs to use for establishing the
initial connection to the Kafka cluster

"""
BOOTSTRAP_SERVERS = "localhost:9092"

"""
An identifier for the stream processing application.
Must be unique within the Kafka cluster.
It is used as:
    1) the default client-id prefix
    2) the group-id for membership management
    3) the changelog topic prefix.

"""
APPLICATION_ID = "wkstream.application.id"

#### - Optional Options - ####

"""
The replication factor for changelog topics and repartition topics created by the application
Default: 1
Importance: Low
"""
REPLICATION_FACTOR = 1

"""
Directory location for state stores
Default: /var/lib/kafka-streams
Importance: Low
"""
STATE_DIR = "/var/lib/kafka-streams"

"""
Maximum number of memory bytes to be used for record caches across all threads
Default: 10485760 (bytes)
Importance: Medium
"""
CACHE_MAX_BYTES_BUFFERING = 10485760

"""
The number of standby replicas for each task
Default: 0
Importance: Medium
"""
NUM_STANDBY_REPLICAS = 0

"""
The number of threads to execute stream processing
Default: 1
Importance: Medium
"""
NUM_STREAM_THREADS = 1

"""
Timestamp extractor class that implements the TimestampExtractor interface
Default: see Timestamp Extractor
Importance: Medium
"""
TIMESTAMP_EXTRACTOR = None  #  TODO

"""
A host:port pair pointing to an embedded user defined endpoint that can be used for discovering the locations of state stores within a single Winton Kafka Streams application. The value of this must be different for each instance of the application.
Default ""
Importance: Low
"""
APPLICATION_SERVER = ""

"""
The maximum number of records to buffer per partition
Default: 1000
Importance: Low
"""
BUFFERED_RECORDS_PER_PARTITION = 1000

"""
An id string to pass to the server when making requests. (This setting is passed to the consumer/producer clients used internally by Winton Kafka Streams.)
Default: ""
Importance: Low
"""
CLIENT_ID = ""

"""
The frequency with which to save the position (offsets in source topics) of tasks
Default: 30000 (millisecs)
Importance: Low
"""
COMMIT_INTERVAL_MS = 30_000

"""
A list of classes to use as metrics reporters
Default: []
Importance: Low
"""
METRIC_REPORTERS: List[str] = []

"""
The number of samples maintained to compute metrics.
Default: 2
Importance: Low
"""
METRICS_NUM_SAMPLES = 2

"""
The highest recording level for metrics.
Default: info
Importance: Low
"""
METRICS_RECORDING_LEVEL = 'info'

"""
The window of time a metrics sample is computed over.
Default: 30000 (millisecs)
Importance: Low
"""
METRICS_SAMPLE_WINDOW_MS = 30_000

"""
Partition grouper class that implements the PartitionGrouper interface
Defatult: see Partition Grouper
Importance: Low
"""
PARITION_GROUPER = None  # DEBUG

"""
The amount of time in milliseconds to block waiting for input
Default: 100 (millisecs)
Importance: Low
"""
POLL_MS = 100

"""
The amount of time in milliseconds to wait before deleting state when a partition has migrated
Default: 60000 (millisecs)
Importance: Low
"""
STATE_CLEANUP_DELAY_MS = 60_000

"""
Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift.
Default: 86400000 (millisecons) = 1 day
Importance: Low
"""
WINDOWSTORE_CHANGELOG_ADDITIONAL_RETENTION_MS = 86_000_000

#### - Non streams configuration parameters - ####

"""
linger.ms (low)	Producer
Default: 100
Importance: low
"""
LINGER_MS = 100

"""
Producer
Default: 10
Importance: low
"""
RETRIES = 10

"""
Consumer
Default: earliest
Importance: low
"""
AUTO_OFFSET_RESET = 'earliest'

"""
Consumer
Default: false, see Consumer Auto Commit
Importance: low
"""
ENABLE_AUTO_COMMIT = 'false'

"""
Consumer
Default: Integer.MAX_VALUE
Importance: low
"""
MAX_POLL_INTERVAL_MS = sys.maxsize  # TODO: No max for Python, this is word size - is that correct for Java?

"""
Consumer
Default: 1000
Importance: low
"""
MAX_POLL_RECORDS = 1000

#### - Serdes Configuration - ####

"""
Default serializer/deserializer class for record values, implements the Serde interface (see also key.serdes)
Default: winton_kafka_streams.processor.serialization.serdes.BytesSerde
Importance: Medium
"""
VALUE_SERDE = serde_as_string(BytesSerde)

"""
Default serializer/deserializer class for record keys, implements the Serde interface (see also value.serdes)
Default: winton_kafka_streams.processor.serialization.serdes.BytesSerde
Importance: Medium
"""
KEY_SERDE = serde_as_string(BytesSerde)

# StringSerde - encoding
SERIALIZER_ENCODING = 'utf-8'
DESERIALIZER_ENCODING = 'utf-8'
KEY_SERIALIZER_ENCODING = None
KEY_DESERIALIZER_ENCODING = None
VALUE_SERIALIZER_ENCODING = None
VALUE_DESERIALIZER_ENCODING = None

# StringSerde - error mode
SERIALIZER_ERROR = 'strict'
DESERIALIZER_ERROR = 'strict'
KEY_SERIALIZER_ERROR = None
KEY_DESERIALIZER_ERROR = None
VALUE_SERIALIZER_ERROR = None
VALUE_DESERIALIZER_ERROR = None

# IntegerSerde/LongSerde - byte order
SERIALIZER_BYTEORDER = 'little'
DESERIALIZER_BYTEORDER = 'little'
KEY_SERIALIZER_BYTEORDER = None
KEY_DESERIALIZER_BYTEORDER = None
VALUE_SERIALIZER_BYTEORDER = None
VALUE_DESERIALIZER_BYTEORDER = None

# IntegerSerde/LongSerde - signed integer
SERIALIZER_SIGNED = 'true'
DESERIALIZER_SIGNED = 'true'
KEY_SERIALIZER_SIGNED = None
KEY_DESERIALIZER_SIGNED = None
VALUE_SERIALIZER_SIGNED = None
VALUE_DESERIALIZER_SIGNED = None

# AvroSerde
AVRO_SCHEMA_REGISTRY = None
AVRO_SCHEMA = None
KEY_AVRO_SCHEMA_REGISTRY = None
KEY_AVRO_SCHEMA = None
VALUE_AVRO_SCHEMA_REGISTRY = None
VALUE_AVRO_SCHEMA = None


def read_local_config(config_file):
    if not os.path.exists(config_file):
        raise KafkaStreamsError(f'Config file {config_file} does not exist')

    with open(config_file, 'r') as cf:
        props = javaproperties.load(cf)

    for k, v in props.items():
        ku = k.upper().replace('.', '_')
        if ku not in globals().keys():
            raise KafkaStreamsError(f'Unrecognised property {k} read from config file {config_file}')
        globals()[ku] = v

        log.debug('Config from "%s": %s = %s', config_file, k, v)
