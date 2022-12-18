from zipfile import ZipFile
import pandas as pd

from mysql_database.schema import Airline, AirPlane, Marketing_Group_Airline

with ZipFile('Combined_Flights_2022.parquet.zip', 'r') as zObject:
    zObject.extractall(path='Combined_Flights_2022.parquet')

df = pd.read_parquet('Combined_Flights_2022.parquet', engine='pyarrow')
print(df.columns)


def write_marketing_airline(values: tuple, columns: list):
    Marketing_Group_Airline(**{column.lower(): value for column, value in zip(columns, values)}).save()


foreign_keys_in_airline = {'DOT_ID_Marketing_Airline': lambda x: Marketing_Group_Airline.get(
    Marketing_Group_Airline.dot_id_marketing_airline == x
)}

foreign_keys_in_airplane = {'DOT_ID_Operating_Airline': lambda x: Airline.get(Airline.dot_id_operating_airline == x)}


def write_airline(values: tuple, columns: list, foreign_keys: dict):
    values = [value if column not in foreign_keys.keys() else foreign_keys[column](value)
              for column, value in zip(columns, values)]
    # columns = [column if column not in foreign_keys_in_airline.keys() else column + '_id' for column in columns]
    kwargs = {column.lower(): value for column, value in zip(columns, values)}
    Airline(**kwargs).save()

def write_airplane(values: tuple, columns: list, foreign_keys: dict):
    values = [value if column not in foreign_keys.keys() else foreign_keys[column](value)
              for column, value in zip(columns, values)]
    # columns = [column if column not in foreign_keys_in_airline.keys() else column + '_id' for column in columns]
    kwargs = {column.lower(): value for column, value in zip(columns, values)}
    AirPlane(**kwargs).save()


if __name__ == "__main__":
    columns_list = ['Marketing_Airline_Network',
                    'IATA_Code_Marketing_Airline',
                    'Operated_or_Branded_Code_Share_Partners',
                    'DOT_ID_Marketing_Airline']

    data = df[columns_list].drop_duplicates()
    for item in zip(*[data[name] for name in columns_list]):
        write_marketing_airline(item, columns_list)


    columns_list = ['Airline',
                    'Operating_Airline',
                    'OriginStateName',
                    'DOT_ID_Operating_Airline',
                    'DOT_ID_Marketing_Airline']

    data = df[columns_list].drop_duplicates()
    for item in zip(*[data[name] for name in columns_list]):
        write_airline(item, columns_list, foreign_keys_in_airline)


    columns_list = ['Tail_Number',
                    'DOT_ID_Operating_Airline']

    data = df[columns_list].drop_duplicates()
    for item in zip(*[data[name] for name in columns_list]):
        write_airplane(item, columns_list, foreign_keys_in_airplane)

