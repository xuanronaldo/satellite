from time import strftime, localtime


# a model class for 8-bytes bytes
class Status:
    # 2022-01-01 00:00:00
    standard_timestamp = 1640966400

    def __init__(self, time_values):
        # parse data from bytes
        self.time = self.standard_timestamp + int.from_bytes(time_values[:4], byteorder='big')
        self.sys_cpu_load = round(int.from_bytes(time_values[4:5], byteorder='big') / 2.56, 1)
        self.sys_memory_used = round(int.from_bytes(time_values[5:6], byteorder='big') / 2.56, 1)
        self.iotdb_memory_used = round(int.from_bytes(time_values[6:7], byteorder='big') / 2.56, 1)
        self.iotdb_cpu_load = round(int.from_bytes(time_values[7:8], byteorder='big') / 2.56, 1)

    def __str__(self):
        # toString method
        status = {
            'time': strftime('%Y-%m-%d %H:%M:%S', localtime(self.time)),
            'sys_cpu_load': str(self.sys_cpu_load) + ' %',
            'sys_memory_used': str(self.sys_memory_used) + ' %',
            'iotdb_memory_used': str(self.iotdb_memory_used) + ' %',
            'iotdb_cpu_load': str(self.iotdb_cpu_load) + ' %'
        }
        return str(status)
