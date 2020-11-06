from abc import ABC, abstractmethod

class Datalake(ABC):
    def __init__(self, config_obj):
        self.connect(config_obj)

    @abstractmethod
    def connect(self, config_obj):
        raise NotImplementedError

    @abstractmethod
    def disconnect(self):
        raise NotImplementedError

    @abstractmethod
    def read(self, data):
        raise NotImplementedError

    @abstractmethod
    def write(self, file_name, data, mod):
        raise NotImplementedError

    @abstractmethod
    def ls(self, name=None):
        raise NotImplementedError

    @abstractmethod
    def rename(self, data):
        raise NotImplementedError

    @abstractmethod
    def move(self, data):
        raise NotImplementedError

    @abstractmethod
    def mkdir(self, data):
        raise NotImplementedError

    @abstractmethod
    def delete(self, data):
        raise NotImplementedError