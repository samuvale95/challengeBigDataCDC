from abc import ABC, abstractmethod

class Database(ABC):

    def __init__(self, config_obj:dict):
        self.connect(config_obj)

    @abstractmethod
    def connect(self, config_obj:dict):
        raise NotImplementedError

    @abstractmethod
    def disconnect(self):
        raise NotImplementedError

    @abstractmethod
    def get_data(self, table_name:str) -> iter(dict):
        raise NotImplementedError