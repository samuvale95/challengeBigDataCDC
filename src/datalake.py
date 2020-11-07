from abc import ABC, abstractmethod

class Datalake(ABC):
    """[summary]

    Args:
        ABC ([type]): [description]
    """
    
    def __init__(self, config_obj):
        """[summary]

        Args:
            config_obj ([type]): [description]
        """
        self.dl_conn = self.connect(config_obj)

    @abstractmethod
    def connect(self, config_obj):
        """[summary]

        Args:
            config_obj ([type]): [description]

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError

    @abstractmethod
    def disconnect(self):
        """[summary]

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError

    @abstractmethod
    def read(self, data) -> str:
        """[summary]

        Args:
            data ([type]): [description]

        Raises:
            NotImplementedError: [description]

        Returns:
            str: [description]
        """
        raise NotImplementedError

    @abstractmethod
    def write(self, file_name, data, mod) -> None:
        """[summary]

        Args:
            file_name ([type]): [description]
            data ([type]): [description]
            mod ([type]): [description]

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError

    @abstractmethod
    def ls(self, path=None) -> list:
        """[summary]

        Args:
            path ([type], optional): [description]. Defaults to None.

        Raises:
            NotImplementedError: [description]

        Returns:
            list: [description]
        """
        raise NotImplementedError

    @abstractmethod
    def rename(self, old_path:str, new_path:str) -> None:
        """[summary]

        Args:
            old_path (str): [description]
            new_path (str): [description]

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError

    @abstractmethod
    def move(self, data):
        """[summary]

        Args:
            data ([type]): [description]

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError

    @abstractmethod
    def mkdir(self, data):
        """[summary]

        Args:
            data ([type]): [description]

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, data):
        """[summary]

        Args:
            data ([type]): [description]

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError