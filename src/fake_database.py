from database import Database
from random import randint
import os

class Fake_Database(Database):
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

    def insert(self):
        with open(self.db_conn, 'a') as f:
            f.write(self.__get_random_record())

    def update(self):
        with open(self.db_conn, 'r') as f, open('edit.csv', 'w') as out:
            cnt=sum(1 for line in open(self.db_conn))

            index=0
            update=randint(1,cnt)
            for line in f:
                if(index==update):
                    l = line.split(',')
                    l[-1] = self.__get_random_record().split(',')[-1]
                    out.write(','.join(l))
                else:
                    out.write(line)
                index+=1
            os.rename('edit.csv', self.db_conn)

    def delete(self):
        with open(self.db_conn, 'r') as f, open('edit.csv', 'w') as out:
            cnt=sum(1 for line in open(self.db_conn))

            index=0
            delete=randint(1,cnt)
            for line in f:
                if(index!=delete):
                    out.write(line)
                index+=1
            os.rename('edit.csv', self.db_conn)

    def __get_random_record(self):
        s=['male', 'female']
        first=str(randint(1000, 1000000))
        last=str(randint(1000, 1000000))
        mail='{}.{}@mail.com'.format(first, last)
        sex=s[randint(0,1)]
        ip='{}.{}.{}.{}'.format(randint(0,255), randint(0,255), randint(0,255), randint(0,255))
        return '{},{},{},{},{}\n'.format(first, last, mail, sex, ip)

