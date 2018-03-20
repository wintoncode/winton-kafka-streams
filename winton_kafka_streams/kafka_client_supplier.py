import logging
import pprint

import confluent_kafka as kafka

log = logging.getLogger(__name__)


class KafkaClientSupplier:
    def __init__(self, _config):
        self.config = _config

    def consumer(self):
        log.debug('Starting consumer...')
        # TODO: Must set all config values applicable to a consumer
        consumer_args = {'bootstrap.servers': self.config.BOOTSTRAP_SERVERS,
                               'group.id': self.config.APPLICATION_ID,
                               'default.topic.config': {'auto.offset.reset':
                                                        self.config.AUTO_OFFSET_RESET},
                               'enable.auto.commit': self.config.ENABLE_AUTO_COMMIT}

        log.debug('Consumer Arguments: %s', pprint.PrettyPrinter().pformat(consumer_args))

        return kafka.Consumer(consumer_args)

    def producer(self):
        # TODO: Must set all config values applicable to a producer
        return kafka.Producer({'bootstrap.servers': self.config.BOOTSTRAP_SERVERS})
