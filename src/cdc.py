from abc import ABC, abstractmethod
import os
import glob
from datetime import datetime


class CDC(ABC):

    def __init__(self, data_lake, data_base, arch_type):
        self.arch_typee=arch_type
        self.files_path='{}/'.format(datetime())
        self.data_lake=data_lake
        self.data_base=data_base

    def send_to_dl(self, ):
        while(True):
            list_file = os.listdir(self.files_path)

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

                files = glob.glob(self.files_path)
                for f in files:
                    os.remove(f)
                break

    def capture_changes(self, ):
        pass

