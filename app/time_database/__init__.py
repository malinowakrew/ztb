from peewee import *

db_connection = SqliteDatabase(r'C:\Users\edzia\Desktop\PK\ztb\app\time_database\db\results.db')


class BaseModel(Model):
    class Meta:
        database = db_connection


class TimeResults(BaseModel):
    database_name = CharField()
    operation_type = CharField()
    operation_time = TimeField()
    value = FloatField(null=True)


class ComputerUsageResults(BaseModel):
    database_name = CharField()
    operation_type = CharField()
    operation_time = TimeField()
    cpu_percentage = FloatField(null=True)
    ram_gb = FloatField(null=True)
    ram_memory = FloatField(null=True)


if __name__ == "__main__":
    db_connection.connect()
    db_connection.create_tables([TimeResults, ComputerUsageResults])
    for v in ComputerUsageResults.select():
        print(v.ram_memory)

    for v in TimeResults.select():
        print(v)
