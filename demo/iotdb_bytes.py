from iotdb.iotdb_manager import IoTDBManager
from model.status import Status
from time import sleep

if __name__ == '__main__':
    # create session with IoTDB
    iotdb = IoTDBManager('127.0.0.1', 6667, 'root', 'root')

    '''
    get data by `bytes_getter` method
    '''
    print('===================== get data by `bytes_getter` method =====================')
    # get bytes data from IoTDB
    bytes_data = iotdb.bytes_getter('root.star.computer')

    # do something to send data

    # parse data on the ground
    if bytes_data != b'00000000':
        print('data size: ', len(bytes_data))
        print(Status(bytes_data))

    '''
    get data by `bytes_generator` iterator
    '''
    print('===================== get data by `bytes_generator` iterator =====================')
    generator = iotdb.bytes_generator('root.star.computer')
    for i in range(0, 10):
        bytes_data = generator.__next__()

        # do something to send data

        # parse data on the ground
        if bytes_data != b'00000000':
            print('data size: ', len(bytes_data))
            print(Status(bytes_data))
        sleep(1)

    iotdb.close_session()