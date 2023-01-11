from time import time
from functools import wraps
from app.time_database import TimeResults, ComputerUsageResults
from threading import Thread
from time import sleep
import psutil

import pandas as pd
from zipfile import ZipFile


def measure_cpu_usage(run, database_type, operation_type):
    while run():
        # cpu = psutil.cpu_percent()
        # ram_gb = psutil.virtual_memory()[2]
        # ram_memory = psutil.virtual_memory()[3] / 1000000000
        # timestamp = time()
        # ComputerUsageResults(database_name=database_type,
        #                      operation_type=operation_type,
        #                      operation_time=timestamp,
        #                      cpu_percentage=cpu,
        #                      ram_gb=ram_gb,
        #                      ram_memory=ram_memory).save()
        #
        # print('CPU:', cpu)
        # print('RAM memory % used:', ram_gb)
        # print('RAM Used (GB):', ram_memory)
        # sleep(10)
        pass


def measure_operation_time(operation_type: str, database_type: str):
    def measure_time(fun):
        @wraps(fun)
        def wrap(*args, **kw):

            run_threads = True
            thread = Thread(target=measure_cpu_usage, args=(lambda: run_threads, database_type, operation_type))
            thread.start()

            time_start = time()
            try:
                result = fun(*args, **kw)
            except Exception as error:
                result = False
                print(error)
            time_end = time()
            duration_time = time_end-time_start
            run_threads = False
            thread.join()

            print(f'Calculation time for {fun.__name__} function '
                  f'with operation {operation_type} in {database_type}: {duration_time}')

            timestamp = time()
            TimeResults(database_name=database_type,
                        operation_type=operation_type,
                        operation_time=timestamp,
                        value=duration_time).save()

            return result
        return wrap
    return measure_time


@measure_operation_time(operation_type='none', database_type='none')
def func():
    with ZipFile('Combined_Flights_2022.parquet.zip', 'r') as zObject:
        zObject.extractall(path='Combined_Flights_2022.parquet')

    df = pd.read_parquet('Combined_Flights_2022.parquet', engine='pyarrow')
    print(df.columns)
    df.dropna(inplace=True)


if __name__ == "__main__":
    func()
