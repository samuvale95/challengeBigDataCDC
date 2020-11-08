from datalake import Datalake
import os

class Fake_Datalake(Datalake):
    """This clas implement a fake datalake to provide an example for CDC pattern
    """

    def connect(self, conf_obj):
        """This method provide e way to connect a Datalake

        Args:
            conf_obj (dict): `dl_name`: datalake name

        Returns:
            str: datalake name
        """
        return conf_obj['dl_name']

    def disconnect(self):
        """This method allow to disconnect a Datalake, set dl_conn to None
        """
        self.dl_conn = None

    def read(self, data) -> str:
        """This method implement a read operation

        Args:
            data (str): file name to read

        Returns:
            str: Iterator to a file
        """
        with open('{}/{}'.format(self.dl_conn, data), 'r') as f:
            return f.read()


    def write(self, file_name, data, mod) -> None:
        """This method allow to write a file inside datalake

        Args:
            file_name (str): file name
            data (str): data to write inside a file
            mod (str): allow mod `w` or `a`
        """
        with open('{}/{}'.format(self.dl_conn, file_name), mod) as f:
            f.write(data)

    def ls(self, path=None) -> list:
        """This method allow to get a list of all file contained inside a datalake

        Args:
            path (str, optional): [description]. path to a specific folder inside datalake. Defaults to None.

        Returns:
            list: All file inside a folder
        """
        if(path == None):
            return os.listdir(self.dl_conn)
        os.listdir('{}/{}'.format(self.dl_conn, path))

    def rename(self, old_path:str, new_path:str) -> None:
        """This method allow to rename a file inside a datalake

        Args:
            old_path (str): old file path
            new_path (str): new file path
        """
        os.rename('{}/{}'.format(self.dl_conn, old_path), '{}/{}'.format(self.dl_conn, new_path))

    def move(self, old_path, new_path):
        """This method allow to move a file inside a datalake

        Args:
            old_path (str): old file path
            new_path (str): new file path

        Raises:
            NotImplementedError: This implementation of a class don't use this method
        """
        raise NotImplementedError

    def mkdir(self, data):
        """This method allow to create a folder inside a datalake

        Args:
            data (str): folder name

        Raises:
            NotImplementedError: This implementation of a class don't use this method
        """
        raise NotImplementedError

    def delete(self, data):
        """This method allow to delete a file inside a datalake

        Args:
            data (str): file name
        """
        os.remove('{}/{}'.format(self.dl_conn, data))
