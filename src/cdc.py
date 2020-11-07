from abc import ABC, abstractmethod
import os
from datetime import datetime
import hashlib
import json


class CDC(ABC):
    """The aim of this class is capture DataBase changes from t0 to t1 and send that changes to Data Lake like a files.
    It will be create a file with 'create_file' method for every change happened on Database.
    The files will be put on a temporary directory (path of this folder will be defined on 'config_object' parameter).
    In the end all files contained in folder will be send to Data Lake with 'send_to_dl' method.
    If something will go wrong during execution of method, the previous state off datatlake will be preseve and another attempt will be make.

    Args:
        ABC ([type]): [description]
    """

    def __init__(self, data_lake, data_base, config_obj:dict):
        """[summary]

        Args:
            data_lake ([type]): [description]
            data_base ([type]): [description]
            config_obj (dict): [description]
        """
        self.conf=config_obj
        self.data_lake=data_lake
        self.data_base=data_base

    def send_to_dl(self) -> None:
        """[summary]
        """
        while(True):
            list_file = os.listdir(self.conf['changes_path'])

            for change in list_file:
                with open('{}/{}'.format(self.conf['changes_path'], change), 'r') as f:
                    self.data_lake.write('{}.tmp'.format('.'.join(change.split('.')[:-1])), f.read(), 'w')

            cnt=0
            for f in list_file:
                if'{}.tmp'.format('.'.join(f.split('.')[:-1])) in self.data_lake.ls(): cnt+=1

            if cnt!=len(list_file):
                for f in list_file:
                    self.data_lake.delete('{}.tmp'.format('.'.join(f.split('.')[:-1])))
            else:
                for f in list_file:
                    self.data_lake.rename('{}.tmp'.format('.'.join(f.split('.')[:-1])), f)

                files = os.listdir(self.conf['changes_path'])

                for f in files:
                    os.remove('{}/{}'.format(self.conf['changes_path'],f))
                break

    @abstractmethod
    def file_struct(self, file_name:str, value:dict, operation:str=None) -> str:
        """[summary]

        Args:
            file_name (str): [description]
            value (dict): [description]
            operation (str, optional): [description]. Defaults to None.

        Raises:
            NotImplementedError: [description]

        Returns:
            str: [description]
        """
        raise NotImplementedError

    def create_file(self, file_name:str, value:dict, operation:str=None) -> None:
        """[summary]

        Args:
            file_name (str): [description]
            value (dict): [description]
            operation (str, optional): [description]. Defaults to None.
        """
        path = self.file_struct(file_name, value, operation)
        os.rename(path, './{}/{}'.format(self.conf['changes_path'], path))

    def __find(self, hash, l, t):
        """[summary]

        Args:
            hash (bool): [description]
            l ([type]): [description]
            t ([type]): [description]

        Returns:
            [type]: [description]
        """
        for i in l:
            if(i[t] == hash): return True
        return False

    def __registry_data(self, table_name:str) -> None:
        """[summary]

        Args:
            table_name (str): [description]
        """
        sync = []
        try:
            sync = json.loads(self.data_lake.read('sync.json'))
        except FileNotFoundError:
            pass

        new_sync=[]
        db_data = self.data_base.exec('SELECT * FROM {}'.format(table_name))

        for data in db_data:
            _khash = str(hashlib.sha256(str.encode(','.join([v for (k,v) in data['keys'].items()]))).hexdigest())
            _hash = str(hashlib.sha256(str.encode(','.join([v for (k,v) in data['values'].items()]))).hexdigest())

            if(not self.__find(_khash, sync, 'khash')):
                self.create_file(str(datetime.now()), {_hash: data['keys'], _khash: data['values']}, 'insert')
            else:
                if(self.__find(_khash, sync, 'khash') and (not  self.__find(_hash, sync, 'hash'))):
                    self.create_file(str(datetime.now()), {_hash: data['keys'], _khash: data['values']}, 'update')
            new_sync.append({'khash': _khash, 'hash': _hash})

        if(sync != []):
            delete_row = set([json.dumps(i) for i in sync]).difference(set([json.dumps(i) for i in new_sync]))
            delete_row = [json.loads(i) for i in delete_row]
            for delete in delete_row:
                self.create_file(datetime.now(), {delete['hash']: None, delete['khash']: None}, 'delete')

        self.data_lake.write('sync.json', json.dumps(new_sync), 'w')

    def __log_data(self, table_name:str) -> None:
        """[summary]

        Args:
            table_name (str): [description]
        """
        sync = self.data_lake.read('sync.json')
        db_data = self.data_base.exec('SELECT * FROM {} WHERE {} > {}'.format(table_name, sync['time_column'], sync['last_value']))

        for data in db_data:
            self.create_file(datetime.now(), data)

    def capture_changes(self, table_name:str)-> None:
        """[summary]

        Args:
            table_name (str): [description]
        """
        os.mkdir(self.conf['changes_path'])
        if(self.conf['arch_type'] == 'log_data'): self.__log_data(table_name)
        else: self.__registry_data(table_name)
        os.remove
        self.send_to_dl()
        os.rmdir(self.conf['changes_path'])