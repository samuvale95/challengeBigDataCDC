from abc import ABC, abstractmethod

class Datalake(ABC):
    def __init__(self, config_obj):
        self.dl_conn = self.connect(config_obj)

    @abstractmethod
    def connect(self, config_obj):
        raise NotImplementedError

    @abstractmethod
    def disconnect(self):
        raise NotImplementedError

    @abstractmethod
    def read(self, data) -> str:
        raise NotImplementedError

    @abstractmethod
    def write(self, file_name, data, mod) -> None:
        raise NotImplementedError

    @abstractmethod
    def ls(self, path=None) -> list:
        raise NotImplementedError

    @abstractmethod
    def rename(self, old_path:str, new_path:str) -> None:
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