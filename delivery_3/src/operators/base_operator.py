from abc import ABC, abstractmethod

class BaseOperator(ABC):
    def __init__(self, collection, output_keys, filter_key=None, selectivity=None):
        self.collection = collection
        self.output_keys = output_keys
        self.filter_key = filter_key
        self.selectivity = selectivity or 0.1

    @abstractmethod
    def run(self):
        pass
