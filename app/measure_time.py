from time import time
from functools import wraps
from app.time_database import Results


def measure_operation_time(operation_type: str, database_type: str):
    def measure_time(fun):
        @wraps(fun)
        def wrap(*args, **kw):
            time_start = time()
            result = fun(*args, **kw)
            time_end = time()
            duration_time = time_end-time_start

            print(f'Calculation time for {fun.__name__} function '
                  f'with operation {operation_type} in {database_type}: {duration_time}')

            Results(database_name=database_type,
                    operation_type=operation_type,
                    value=duration_time).save()

            return result
        return wrap
    return measure_time


@measure_operation_time(operation_type='none', database_type='none')
def func():
    for i in range(1, 10000):
        i += 1


if __name__ == "__main__":
    func()
