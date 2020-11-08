from real_cdc import Real_CDC
from fake_database import Fake_Database
from fake_datalake import Fake_Datalake
import os
from shutil import copyfile


def test_dl_num_files(num, name):
    dl_len = len(os.listdir('tmp_dl'))
    assert num == dl_len, '{} FAIL: expected {}, instead value was {}'.format(name, num, dl_len)
    print('{}: TEST OK'.format(name))

if __name__ == "__main__":
    copyfile('MOCK_DATA.csv', 'TEST_MOCK_DATA.csv')

    data_lake = Fake_Datalake({'dl_name':'tmp_dl'})
    data_base = Fake_Database({'db_name':'TEST_MOCK_DATA.csv'})
    cdc = Real_CDC(data_lake, data_base, {
            'arch_type': 'registry_data',
            'changes_path': 'tmp'
        })

    try:
        #Datalake is empty, no changes detected yet
        test_dl_num_files(0, 'Test empty DL')

        #DB is firt time run, all db line will be processed and put on datalake
        cdc.capture_changes('')

        #20 line for datalake was changed
        test_dl_num_files(21, 'Firt DB Check')

        #insert 3 line, 3 file will be insert on datalake
        for i in range(0, 3):
            data_base.insert()

        cdc.capture_changes('')

        test_dl_num_files(24, 'Insert test')

        #delete one row, one file will be put on datalake
        data_base.delete()

        cdc.capture_changes('')

        test_dl_num_files(25, 'Delete Test')

        # one line updated, one file wil be put on datalake
        data_base.update()

        cdc.capture_changes('')

        test_dl_num_files(26, 'Update Test')

        data_base.update()
        data_base.insert()
        data_base.delete()

        cdc.capture_changes('')

        test_dl_num_files(29, 'Update, Insert, Delete Test')

    except AssertionError as ass_err:
        for f in os.listdir('tmp_dl'):
            os.remove('{}/{}'.format('tmp_dl',f))
        print(ass_err)

    os.remove('TEST_MOCK_DATA.csv')
