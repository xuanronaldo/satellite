from manager.iotdb_manager import IoTDBManager
from time import sleep

if __name__ == '__main__':
    source_iotdb = IoTDBManager('127.0.0.1', 6667, 'root', 'root')
    target_iotdb = IoTDBManager('127.0.0.1', 6668, 'root', 'root')

    last_value_generator = source_iotdb.last_value_generator('root.star.computer')
    for i in range(0, 10):
        df = last_value_generator.__next__()
        print('sync data: ')
        print(df)
        target_iotdb.insert_last_value('root.star.computer', df)
        sleep(1)

    source_iotdb.close_session()
    target_iotdb.close_session()
