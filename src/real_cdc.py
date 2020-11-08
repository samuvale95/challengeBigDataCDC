from cdc import CDC
from datetime import datetime
import json

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