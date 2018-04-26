class StoreChangeLogger:
    def __init__(self, store_name, context) -> None:
        self.topic = f'{context.application_id}-{store_name}-changelog'
        self.context = context
        self.partition = context.task_id.partition
        self.record_collector = context.state_record_collector

    def log_change(self, key: bytes, value: bytes) -> None:
        if self.record_collector:
            self.record_collector.send(self.topic, key, value, self.context.timestamp, partition=self.partition)
