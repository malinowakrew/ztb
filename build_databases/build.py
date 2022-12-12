from zipfile import ZipFile
import pandas as pd

from mysql_database.schema import Airline, AirPlane, Marketing_Airline

with ZipFile('Combined_Flights_2022.parquet.zip', 'r') as zObject:
    zObject.extractall(path='Combined_Flights_2022.parquet')

df = pd.read_parquet('Combined_Flights_2022.parquet', engine='pyarrow')
print(df.columns)


def write_marketing_airline(values: tuple, columns: list):
    Marketing_Airline(**{column.lower(): value for column, value in zip(columns, values)}).save()


if __name__ == "__main__":
    columns_list = ['Marketing_Airline_Network',
                    'IATA_Code_Marketing_Airline',
                    'Operated_or_Branded_Code_Share_Partners',
                    'DOT_ID_Marketing_Airline']

    data = df[columns_list].drop_duplicates()
    for item in zip(*[data[name] for name in columns_list]):
        write_marketing_airline(item, columns_list)
