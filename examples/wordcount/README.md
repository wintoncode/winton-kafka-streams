# Wordcount Winton Kafka Streams Example

## Running native
* Edit the config.properties file if necessary to change where Kafka is running
* Run: python example.py
* Start a console producer writing to the topic 'wks-wordcount-example-topic'

##Running dockerized
* Install docker and docker-compose
* cd into examples/wordcount/docker
* start the docker services with `docker-compose up -d`
* the kafka-debug service outputs the `wks-wordcount-example-count`
* the output should be (after a minute or so):
    ```
    $ docker-compose logs kafka-debug
    ...
    kafka-debug_1    | b	2
    kafka-debug_1    | c	1
    kafka-debug_1    | a	3
    ```

## Features
* Listens to the topic 'wks-wordcount-example-topic' and writes output to 'wks-wordcount-example-count'
* Each string read in will be split by spaces
* The count of the number of words will be maintained in a collections.Counter instance. This is not persistent so stopping the example will not maintain the previous state. 
