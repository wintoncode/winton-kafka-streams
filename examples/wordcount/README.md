# Wordcount Winton Kafka Streams Example

## Running
* Edit the config.properties file if necessary to change where Kafka is running
* Run: python example.py
* Start a console producer writing to the topic 'wks-wordcount-example-topic-two'

## Features
* Listens to the topic 'wks-wordcount-example-topic' and writes output to 'wks-wordcount-example-count'
* Each string read in will be split by spaces
* The count of the number of words will be maintained in a collections.Counter instance. This is not persistent so stopping the example will not maintain the previous state. 
