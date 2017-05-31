"""
Configuration of Apache Kafka

"""

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
APLICATION_ID = "application.id"

# Common options for configuration


# Etc.

