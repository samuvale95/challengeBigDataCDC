from cdc import CDC
from database import Database as db
from datalake import Datalake as dl
from datetime import datetime
import json
import os
import time

class Real_CDC(CDC):
    def file_struct(self, file_name, value, operation=None):
        with open('{}.json'.format(file_name), 'w') as f:
            if(self.conf['arch_type'] == 'log_data'):
                data = {
                    'time_stamp': str(datetime.now()),
                    'db_value': value
                }
            else:
                data = {
                    'time_stamp': str(datetime.now()),
                    'db_value': value,
                    'op_type': operation
                }
            f.write(json.dumps(data))
        return f.name


class Mock_Registry_Database(db):
    def connect(self, config_obj):
        return config_obj['db_name']

    def disconnect(self):
        self.db_conn = None

    def exec(self, query:str) -> list:
        with open(self.db_conn, 'r') as f:
            result = list()
            head = f.readline()
            for line in f:
                line = line.split(',')
                result.append({
                        'keys': {
                            head[0]: line[0],
                            head[1]: line[1]
                        },
                        'values': {
                            head[2]: line[2],
                            head[3]: line[3],
                            head[4]: line[4]
                        }
                    })
        return result

class Real_Datalake(dl):

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

if __name__ == "__main__":
    data_lake = Real_Datalake({'dl_name':'tmp_dl'})
    data_base = Mock_Registry_Database({'db_name':'MOCK_DATA.csv'})
    cdc = Real_CDC(data_lake, data_base, {
            'arch_type': 'registry_data',
            'changes_path': 'tmp'
        })

    cdc.capture_changes('')