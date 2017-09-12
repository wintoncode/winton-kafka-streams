FROM python:3.6
ADD . /code

RUN apt-get update
RUN echo "/usr/local/lib" >> /etc/ld.so.conf
RUN git clone https://github.com/edenhill/librdkafka.git /tmp/librdkafka
RUN ls /tmp/ && cd /tmp/librdkafka && ./configure && make && make install && ldconfig

WORKDIR /code/examples/wordcount/
RUN pip --version
#RUN pip install -e git+https://github.com/confluentinc/confluent-kafka-python.git#egg=confluent-kafka
RUN pip install -e ../../

CMD ["python", "source_client.py"]
