from database import Database
from random import randint
import os

class Fake_Database(Database):
    """This clas implement a fake database to provide an example for CDC pattern
    """
    def connect(self, config_obj):
        """[summary]

        Args:
            config_obj (dict): `db_name`: databse name

        Returns:
            str: return a db_name use like a connection object
        """
        return config_obj['db_name']

    def disconnect(self):
        """Set db_conn to None
        """
        self.db_conn = None

    def exec(self, query:str) -> list:
        """This method execute a fake query and return all databse row

        Args:
            query (str): Not use for this example

        Returns:
            list: Iterator that point to first line of result
        """

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
        """This method made a random insert to provide a way to make some test
        """

        with open(self.db_conn, 'a') as f:
            f.write(self.__get_random_record())

    def update(self):
        """This method made a random update to provide a way to make some test
        """

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
        """This method made a random delete to provide a way to make some test
        """

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
        """This method return a random record based on a MOCK_DATA.csv structure

        Returns:
            str: return a random row value in a csv style
        """

        s=['male', 'female']
        first=str(randint(1000, 1000000))
        last=str(randint(1000, 1000000))
        mail='{}.{}@mail.com'.format(first, last)
        sex=s[randint(0,1)]
        ip='{}.{}.{}.{}'.format(randint(0,255), randint(0,255), randint(0,255), randint(0,255))
        return '{},{},{},{},{}\n'.format(first, last, mail, sex, ip)

