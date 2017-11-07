# Binning example

## Additional Python Package

In addition to the packages required by the Winton Kafka Streams package, you
will also need to have `pandas` available.  See also `live-plot.ipynb`
for a jupyter notebook visualising this example (it additionally
requires `jupyter` and `bokeh`).

## Prepare Kafka

Start up Zookeeper and Kafka:

    bin/zookeeper-server-start.sh config/zookeeper.properties
    bin/kafka-server-start.sh config/server.properties

Then create the topics used in this example:

    bin/kafka-topics.sh \
       --create \
       --zookeeper localhost:2181 \
       --replication-factor 1 \
       --partitions 1 \
       --topic prices
       
    bin/kafka-topics.sh \
        --create \
        --zookeeper localhost:2181 \
        --replication-factor 1 \
        --partitions 1 \
        --topic bin-prices

## Run generator

First generate a log of the full data - see `python generator.py --help`
for details of the options:

    python generator.py \
        -i AAA,0.3,123,100.0,0.01 \
        -i BBB,0.4,456,70.0,0.011 \
        -l 60000 -f 250ms \
    > full_data.log

then run again, but this time producing 'in real-time' to a Kafka topic
(the generated data is the same as above, as the setup is the same):

    python generator.py \
        -i AAA,0.3,123,100.0,0.01 \
        -i BBB,0.4,456,70.0,0.011 \
        -l 6000 -f 1s \
        -kb localhost:9092 -kt prices \
        -rt 

This will produce outputs to the 'bin-prices' topic.

In both cases the script will terminated once any of the two items has
produced 60000 values.

## Run binning

Now run the Winton Kafka Streams application that consumes the price topic:

    python -u binning.py --config-file config.properties

Note: When terminating it, the last 2 bins are likely to be missing from
the output - this needs improvement.

## Inspect output

A simple way to see the produced results is to run:

    kafkacat -b localhost -t bin-prices -e -J

which will print a JSON formatted view of the topic to stdout.
