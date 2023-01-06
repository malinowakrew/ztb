from zipfile import ZipFile
import pandas as pd
import mongo_database.schema as mongo_schema
from mongoengine import connect, disconnect

disconnect()

connect(
    db='test',
    host='mongodb://test:test@localhost:27018/'
)


class_objects_mongo = {
    'Marketing_Group_Airline': lambda kwargs: mongo_schema.Marketing_Group_Airline(**kwargs).save(),
    'Airline': lambda kwargs: mongo_schema.Airline(**kwargs).save(),
    'Airplane': lambda kwargs: mongo_schema.Airplane(**kwargs).save(),
    'Airport': lambda kwargs: mongo_schema.Airport(**kwargs).save(),
    'Flight': lambda kwargs: mongo_schema.Flight(**kwargs).save()
}


def write_item(values: tuple, columns: list, class_type: str, foreign_keys: dict):
    values = [value if column not in foreign_keys.keys() else foreign_keys[column](value)
              for column, value in zip(columns, values)]

    kwargs = {column.lower(): value for column, value in zip(columns, values)}

    class_objects_mongo[class_type](kwargs)


def write_table(df: pd.DataFrame, columns: list, class_type: str, foreign_keys: dict = {}):
    data = df[columns].drop_duplicates()

    for item in zip(*[data[name] for name in columns]):
        write_item(item, columns, class_type, foreign_keys)


foreign_keys_in_airline = {'DOT_ID_Marketing_Airline':
                               lambda x: mongo_schema.Marketing_Group_Airline.objects(dot_id_marketing_airline=x)}

foreign_keys_in_airplane = {'DOT_ID_Operating_Airline':
                                lambda x: mongo_schema.Airline.objects(dot_id_operating_airline=x)}

foreign_keys_in_flight = {'Tail_Number': lambda x: mongo_schema.Airplane.objects(tail_number=x),
                          'OriginCityName': lambda x: mongo_schema.Airport.objects(origincityname=x),
                          'DestCityName': lambda x: mongo_schema.Airport.objects(origincityname=x)}


if __name__ == "__main__":
    # with ZipFile('Combined_Flights_2022.parquet.zip', 'r') as zObject:
    #     zObject.extractall(path='Combined_Flights_2022.parquet')
    #
    # df = pd.read_parquet('Combined_Flights_2022.parquet', engine='pyarrow')
    # print(df.columns)
    #
    # columns_list = ['Marketing_Airline_Network',
    #                 'IATA_Code_Marketing_Airline',
    #                 # 'Operated_or_Branded_Code_Share_Partners',
    #                 'DOT_ID_Marketing_Airline']
    # write_table(df, columns_list, 'Marketing_Group_Airline')
    #
    # columns_list = ['Airline',
    #                 'Operating_Airline',
    #                 # 'OriginStateName',
    #                 'DOT_ID_Operating_Airline',
    #                 'DOT_ID_Marketing_Airline']
    # write_table(df, columns_list, 'Airline', foreign_keys_in_airline)
    #
    # columns_list = ['Tail_Number',
    #                 'DOT_ID_Operating_Airline']
    # write_table(df, columns_list, 'Airplane', foreign_keys_in_airplane)
    #
    # # columns_list = ['Distance']
    # # write_table(df, columns_list, 'Flight')
    with ZipFile('Combined_Flights_2022.parquet.zip', 'r') as zObject:
        zObject.extractall(path='Combined_Flights_2022.parquet')

    df = pd.read_parquet('Combined_Flights_2022.parquet', engine='pyarrow')
    print(df.columns)
    df.dropna(inplace=True)

    columns_list = ['Marketing_Airline_Network',
                    'IATA_Code_Marketing_Airline',
                    'Operated_or_Branded_Code_Share_Partners',
                    'DOT_ID_Marketing_Airline']
    write_table(df, columns_list, 'Marketing_Group_Airline')

    columns_list = ['Airline',
                    'Operating_Airline',
                    'DOT_ID_Operating_Airline',
                    'DOT_ID_Marketing_Airline']
    write_table(df, columns_list, 'Airline', foreign_keys_in_airline)

    columns_list = ['Tail_Number',
                    'DOT_ID_Operating_Airline']
    write_table(df, columns_list, 'Airplane', foreign_keys_in_airplane)

    df_temp = pd.concat(
        [df[['OriginCityName', 'OriginStateName']],
         df[['DestCityName', 'OriginStateName']].rename(columns={'DestCityName': 'OriginCityName'})],
        axis=0, ignore_index=True)
    # df_temp.columns = ['OriginCityName']
    df_temp = df_temp.drop_duplicates(keep='first')
    # df_exp = df_temp['OriginCityName'].str.split(pat=',', expand=True)
    df_temp.columns = ['OriginCityName', 'StateName']

    columns_list = ['OriginCityName',
                    'StateName']
    write_table(df_temp, columns_list, 'Airport')

    columns_list = ['FlightDate',
                    'OriginCityName',
                    'DestCityName',
                    'AirTime',
                    'Distance',
                    'Tail_Number',
                    'Flight_Number_Operating_Airline']
    write_table(df, columns_list, 'Flight', foreign_keys_in_flight)
