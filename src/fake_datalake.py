from datalake import Datalake
import os

class Fake_Datalake(Datalake):

    def connect(self, conf_obj):
        return conf_obj['dl_name']

    def disconnect(self):
        self.dl_conn = None

    def read(self, data) -> str:
        with open('{}/{}'.format(self.dl_conn, data), 'r') as f:
            return f.read()


    def write(self, file_name, data, mod) -> None:
        with open('{}/{}'.format(self.dl_conn, file_name), mod) as f:
            f.write(data)

    def ls(self, path=None) -> list:
        if(path == None):
            return os.listdir(self.dl_conn)
        os.listdir('{}/{}'.format(self.dl_conn, path))

    def rename(self, old_path:str, new_path:str) -> None:
        os.rename('{}/{}'.format(self.dl_conn, old_path), '{}/{}'.format(self.dl_conn, new_path))

    def move(self, data):
        raise NotImplementedError

    def mkdir(self, data):
        raise NotImplementedError

    def delete(self, data):
        os.remove('{}/{}'.format(self.dl_conn, data))
