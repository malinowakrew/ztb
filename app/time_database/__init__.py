from peewee import *

db_connection = SqliteDatabase(r'app\time_database\db\results.db')


class BaseModel(Model):
    class Meta:
        database = db_connection


class Results(BaseModel):
    database_name = CharField()
    operation_type = CharField()
    value = FloatField()


if __name__ == "__main__":
    db_connection.connect()
    db_connection.create_tables([Results])
