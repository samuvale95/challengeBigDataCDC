from abc import ABC, abstractmethod

class Datalake(ABC):
    """This class is a template to implement a real Datalake class
    """

    def __init__(self, config_obj):
        """Datalake constructor

        Args:
            config_obj (dict): configuration object depend on implementation
        """
        self.dl_conn = self.connect(config_obj)

    @abstractmethod
    def connect(self, config_obj):
        """Connection method

        Raises:
            NotImplementedError: This method must be implemented in a concrete class
        """
        raise NotImplementedError

    @abstractmethod
    def disconnect(self):
        """Disconnect method

        Raises:
            NotImplementedError: This method must be implemented in a concrete class
        """
        raise NotImplementedError

    @abstractmethod
    def read(self, data) -> str:
        """This method allow to read some file from datalake

        Args:
            data (str): path to file

        Raises:
            NotImplementedError: This method must be implemented in a concrete class

        Returns:
            str: Return an iterator that point to first line of file
        """
        raise NotImplementedError

    @abstractmethod
    def write(self, file_name, data, mod) -> None:
        """This method allow to write in append or write mode a file inside datalake

        Args:
            file_name (str): file name
            data (str): value to write inside a file
            mod (str): mod allow are `a` or `w`

        Raises:
            NotImplementedError: This method must be implemented in a concrete class
        """
        raise NotImplementedError

    @abstractmethod
    def ls(self, path=None) -> list:
        """[summary]

        Args:
            path ([type], optional): [description]. Defaults to None.

        Raises:
            NotImplementedError: This method must be implemented in a concrete class

        Returns:
            list: [description]
        """
        raise NotImplementedError

    @abstractmethod
    def rename(self, old_path:str, new_path:str) -> None:
        """This method allow to rename a file inside datalake

        Args:
            old_path (str): old path to file
            new_path (str): new path to file

        Raises:
            NotImplementedError: This method must be implemented in a concrete class
        """
        raise NotImplementedError

    @abstractmethod
    def move(self, old_path, new_path):
        """This method allow to move some file from a pth to another

        Args:
            old_path(str): old path to file
            new_path(str): new path to file

        Raises:
            NotImplementedError: This method must be implemented in a concrete class
        """
        raise NotImplementedError

    @abstractmethod
    def mkdir(self, data):
        """This method allow to create a new folder inside datalake

        Args:
           data (str): folder name

        Raises:
            NotImplementedError: This method must be implemented in a concrete class
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, data):
        """This method allow to delete a file inside a datalake

        Args:
            data (str): file name

        Raises:
            NotImplementedError: This method must be implemented in a concrete class
        """
        raise NotImplementedError