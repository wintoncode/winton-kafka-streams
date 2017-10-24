class TaskId:
    def __init__(self, topic_group_id, partition):
        self.topic_group_id = topic_group_id
        self.partition = partition

    def __repr__(self):
        return f"{self.topic_group_id}_{self.partition}"

    def __eq__(self, other):
        if other.__class__ is self.__class__:
            return (self.topic_group_id, self.partition) == (other.topic_group_id, other.partition)
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.topic_group_id, self.partition))
