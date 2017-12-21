from queue import PriorityQueue
from collections import namedtuple


class PunctuationSchedule(namedtuple('PunctuationSchedule', ['timestamp', 'node', 'interval'])):
    def __lt__(self, other):
        return self.timestamp < other.timestamp


class PunctuationQueue:

    def __init__(self, punctuate):
        self.pq = PriorityQueue()
        self.punctuate = punctuate

    def schedule(self, node, interval):
        self.pq.put(PunctuationSchedule(0, node, interval))

    def may_punctuate(self, timestamp):
        punctuated = False
        while not self.pq.empty():
            top = self.pq.get()
            if top.timestamp <= timestamp:
                self.punctuate(top.node, timestamp)
                punctuated = True
                next_timestamp = top.interval + (timestamp if top.timestamp == 0 else top.timestamp)
                self.pq.put(PunctuationSchedule(next_timestamp, top.node, top.interval))
            else:
                self.pq.put(top)
                break
        return punctuated
