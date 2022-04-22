from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType
from functools import reduce
from time import sleep


class IoTDBManager:
    session = None
    type_dict = {
        'BOOLEAN': TSDataType.BOOLEAN,
        'INT32': TSDataType.INT32,
        'INT64': TSDataType.INT64,
        'FLOAT': TSDataType.FLOAT,
        'DOUBLE': TSDataType.DOUBLE,
        'TEXT': TSDataType.TEXT
    }

    def __init__(self, ip, port, username, password):
        self.session = Session(ip, port, username, password)
        self.session.open(False)

    def get_last_value_of_device(self, device):
        query = 'select last(*) from %s' % device
        try:
            df = self.session.execute_query_statement(query).todf()
            return df
        except Exception as e:
            print(e)
            return None

    def last_value_generator(self, device, time_interval=1):
        while True:
            yield self.get_last_value_of_device(device)
            sleep(time_interval)

    def bytes_getter(self, device):
        # 2022-01-01 00:00:00
        standard_timestamp = 1640966400

        df = self.get_last_value_of_device(device)
        if df is not None:
            time = (int(df['Time'].get(0) / 1000) - standard_timestamp).to_bytes(4, byteorder='big')
            values = df['value'].map(lambda x: int(round(float(x) * 2.56)).to_bytes(1, byteorder='big'))
            values = reduce(lambda x, y: x.__add__(y), values)
            return time.__add__(values)
        else:
            return b'00000000'

    def bytes_generator(self, device, time_interval=1):
        while True:
            yield self.bytes_getter(device)
            sleep(time_interval)

    def insert_last_value(self, device, df):
        time = df['Time'][0]
        measurements = df['timeseries'].map(lambda x: x.lstrip(device + '.')).tolist()
        types = df['dataType'].map(lambda x: self.type_dict[x]).tolist()
        values = []
        for index, row in df.iterrows():
            values.append(self.trans_type(row['dataType'], row['value']))
        self.session.insert_record(device, time, measurements, types, values)

    def close_session(self):
        self.session.close()

    @staticmethod
    def trans_type(type_, data):
        if type_ == 'BOOLEAN':
            return bool(data)
        elif type_ == 'INT32' or type_ == 'INT64':
            return int(data)
        elif type_ == 'FLOAT' or type_ == 'DOUBLE':
            return float(data)
        elif type_ == 'TEXT':
            return str(data)


if __name__ == '__main__':
    iotdb = IoTDBManager('127.0.0.1', 6667, 'root', 'root')
    df = iotdb.get_last_value_of_device('root.star.computer')
    iotdb.insert_last_value('root.star.computer', df)

    iotdb.close_session()
