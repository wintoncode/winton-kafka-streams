# Binning example

## Run generator

First generate a log of the full data - see `python generator.py --help`
for details of the options:

    python generator.py \
        -i AAA,0.3,123,100.0,0.01 \
        -i BBB,0.4,456,100.0,0.011 \
        -l 6000 -f 1s \
    > full_data.log

then run again, but this time producing to a Kafka topic (the generated
data is the same as above, as the setup is the same):

    python generator.py \
        -i AAA,0.3,123,100.0,0.01 \
        -i BBB,0.4,456,100.0,0.011 \
        -l 6000 -f 1s \
        -kb localhost:9092 -kt price

In both cases the scrip will terminated once any of the two items has
produce 6000 values.

## Run binning

Now run the Kafka Streams application that consumes the price topic:

    python -u binning.py --config-file config.properties > binned_data.log

Note: When terminating it, the last 2 bins are likely to be missing from
the output - this needs improvement.
