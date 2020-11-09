from abc import ABC, abstractmethod

class Database(ABC):
    """This class is a template to implement a real Database class
    """

    def __init__(self, config_obj:dict):
        """Database constructor

        Args:
            config_obj (dict): configuration object depends on implementation
        """
        self.db_conn = self.connect(config_obj)

    @abstractmethod
    def connect(self, config_obj:dict):
        """This method allow to connect a database

        Args:
            config_obj (dict): configuration object

        Raises:
            NotImplementedError: This method must be implemented in a concrete class
        """
        raise NotImplementedError

    @abstractmethod
    def disconnect(self):
        """This method allow to disconnect the database

        Raises:
            NotImplementedError: This method must be implemented in a concrete class
        """
        raise NotImplementedError

    @abstractmethod
    def exec(self, query:str) -> list:
        """This method allow to execute a query to database

        Args:
            query (str): query string

        Raises:
            NotImplementedError: This method must be implemented in a concrete class

        Returns:
            list: Iterator that point to query result
        """
        raise NotImplementedError