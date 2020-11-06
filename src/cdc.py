from abc import ABC, abstractmethod
import os
import glob
from datetime import datetime
import hashlib


class CDC(ABC):

    def __init__(self, data_lake, data_base, config_obj:dict):
        self.conf=config_obj
        self.data_lake=data_lake
        self.data_base=data_base

    def send_to_dl(self) -> None:
        while(True):
            list_file = os.listdir(self.conf['changes_path'])

            for change in list_file:
                with open(change, 'r') as f:
                    self.data_lake.write(str.encode(f.read()), '{}.tmp'.format(change.split('.')[0]), 'w')

            cnt=0
            for f in list_file:
                if self.data_lake.ls('{}.tmp'.format(f.split('.')[0])): cnt+=1

            if cnt!=len(list_file):
                for f in list_file:
                    self.data_lake.remove('{}.tmp'.format(f.split('.')[0]))
            else:
                for f in list_file:
                    self.data_lake.rename('{}.tmp'.format(f.split('.')[0]), f)

                files = glob.glob(self.conf['changes_path'])
                for f in files:
                    os.remove(f)
                break

    @abstractmethod
    def create_file(self, name:str, value:str, operation:str) -> None:
        raise NotImplementedError

    def __find(self, hash, l, t):
        for i in l:
            if(i[t] == hash): return True
        return False

    def __registry_data(self, table_name:str) -> None:
        sync = self.data_lake.read('sync.json')
        new_sync=[]
        db_data = self.data_base.get_data(table_name)

        for data in db_data:
            _khash = hashlib.sha256(str.encode([v for (k,v) in data['keys'].items()].join(','))).hexdigest()
            _hash = hashlib.sha256(str.encode([v for (k,v) in data['values'].items()].join(','))).hexdigest()

            if(not self.__find(_khash, sync, 'khash')):
                self.create_file(datetime.now(), {_hash: data['keys'], _khash: data['values']}, 'insert')
            else:
                if(self.__find(_khash, sync, 'khash') and (not  self.__find(_hash, sync, 'hash'))):
                    self.create_file(datetime.now(), {_hash: data['keys'], _khash: data['values']}, 'update')
            new_sync.append({'khash': _khash, 'hash': _hash})

            delete_row = set(new_sync).intersection(set(sync))
            for delete in delete_row:
                self.create_file(datetime.now(), {delete['hash']: None, delete['khash']: None}, 'delete')

    def __log_data(self, table_name:str) -> None:
        return NotImplemented

    def capture_changes(self, table_name:str)-> None:
        if(self.conf['arch_type'] == 'log_data'): self.__log_data(table_name)
        else: self.__registry_data(table_name)