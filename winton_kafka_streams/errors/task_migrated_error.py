from .kafka_streams_error import KafkaStreamsError


class TaskMigratedError(KafkaStreamsError):
    """
    Indicates that a task got migrated to another thread.
    Thus, the task raising this exception can be cleaned up and closed as "zombie".
    """
    pass
