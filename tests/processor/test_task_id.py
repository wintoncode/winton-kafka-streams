from winton_kafka_streams.processor.task_id import TaskId


def test_taskId():
    task_id = TaskId('group1', 0)

    assert task_id == TaskId('group1', 0)
    assert not (task_id != TaskId('group1', 0))
    assert task_id != TaskId('group1', 1)
    assert task_id != TaskId('group2', 0)

    assert repr(task_id) == 'group1_0'

    assert hash(task_id) == hash(TaskId('group1', 0))
