# Debug Winton Kafka Streams Example

## Running
* Edit the config.properties file if necessary to change where Kafka is running
* Run: python example.py
* Start a console producer writing to the topic 'wks-debug-example-topic-two'

## Features
* Listens to the topic 'wks-debug-example-topic-two' and writes output to 'wks-debug-example-output'
* The value on the input topic will be doubled when received
* Every fourth value will cause the four values in the current state to be written to the output topic
* It is possible to stop the application at any time and the application will restart where it left off
