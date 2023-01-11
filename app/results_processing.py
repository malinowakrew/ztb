import measure_time
import plotly.express as px
import time_database as time_schemas
import pandas as pd


def create_box_plot(operation_type: str):
    query = time_schemas.TimeResults.select().where(time_schemas.TimeResults.operation_type == operation_type)
    df = pd.DataFrame(query.dicts())
    fig = px.box(df, x='database_name', y='value', points='all',
                 labels={
                     'database_name': 'Typ bazy danych',
                     'value': 'Czas wykonania zapytania',
                 },
                 title=f'Czas wykoniania operacji {operation_type} dla obu typów baz'
                 )
    fig.show()


def create_scatter_plot(operation_type: str):
    query = time_schemas.TimeResults.select().where(time_schemas.TimeResults.operation_type == operation_type)
    df = pd.DataFrame(query.dicts())
    print(df)
    fig = px.scatter(df, x='number_of_rows', y='value', color='database_name',
                     title=f'Czas wykoniania operacji {operation_type} dla obu typów baz',
                     labels={
                         'database_name': 'Typ bazy danych',
                         'value': 'Czas wykonania zapytania',
                     })
    fig.show()


if __name__ == "__main__":
    # create_box_plot('Ładowanie')
    create_scatter_plot('Ładowanie')

