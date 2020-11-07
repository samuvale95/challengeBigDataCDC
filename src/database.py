from abc import ABC, abstractmethod

class Database(ABC):
    """[summary]

    Args:
        ABC ([type]): [description]
    """

    def __init__(self, config_obj:dict):
        """[summary]

        Args:
            config_obj (dict): [description]
        """
        self.db_conn = self.connect(config_obj)

    @abstractmethod
    def connect(self, config_obj:dict):
        """[summary]

        Args:
            config_obj (dict): [description]

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
    def exec(self, query:str) -> list:
        """[summary]

        Args:
            query (str): [description]

        Raises:
            NotImplementedError: [description]

        Returns:
            list: [description]
        """
        raise NotImplementedError