"""
Configuration of Apache Kafka

"""

import os

from ._error import KafkaStreamsError

# Required options

"""
The kafka server(s) from which sink/source processors will be connected

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

TASK_ID = "wkstream.task.id"

# Common options for configuration


# Etc.



def read_local_config(config_file):
    if config_file is not None:
        cfmod = __import__(config_file, globals(), locals(), ['*'])
