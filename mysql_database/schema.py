from peewee import *

db = MySQLDatabase('test', host='localhost', port=3307, user='test', password='test')


class BaseModel(Model):
    class Meta:
        database = db


class Marketing_Airline(BaseModel):
    marketing_airline_network = CharField()
    iata_code_marketing_airline = CharField()
    operated_or_branded_code_share_partners = CharField()
    dot_id_marketing_airline = IntegerField()


class Airline(BaseModel):
    airline = CharField()
    operating_airline = CharField()
    origin_state_name = CharField()
    dot_id_operating_airline = IntegerField()
    dot_id_marketing_airline = ForeignKeyField(Marketing_Airline, to_field='dot_id_marketing_airline')


class AirPlane(BaseModel):
    tail_number = CharField()
    total_air_time = FloatField()
    dot_id_operating_airline = ForeignKeyField(Airline, to_field='dot_id_operating_airline')


if __name__ == "__main__":
    # db.connect()
    # dq = Marketing_Airline.delete()
    # dq.execute()
    # db.create_tables([Marketing_Airline])

    inst = Marketing_Airline.select()#.where(Plane.mileage >= 10)
    for o in inst:
        print(o.marketing_airline_network)
