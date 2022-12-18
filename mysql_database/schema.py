from peewee import *
import datetime

db = MySQLDatabase('test', host='localhost', port=3307, user='test', password='test')


class BaseModel(Model):
    class Meta:
        database = db


class Marketing_Group_Airline(BaseModel):
    marketing_airline_network = CharField()
    iata_code_marketing_airline = CharField()
    operated_or_branded_code_share_partners = CharField()
    dot_id_marketing_airline = IntegerField()


class Airline(BaseModel):
    airline = CharField()
    operating_airline = CharField()
    origin_state_name = CharField()
    dot_id_operating_airline = IntegerField()
    dot_id_marketing_airline = ForeignKeyField(Marketing_Group_Airline,
                                               backref='market_group',
                                               lazy_load=False)
                                               # to_field='dot_id_marketing_airline')


class AirPlane(BaseModel):
    tail_number = CharField(null=True)
    # total_air_time = FloatField()
    dot_id_operating_airline = ForeignKeyField(Airline,
                                               backref='airline',
                                               lazy_load=False)


if __name__ == "__main__":
    # db.connect()
    # dq = AirPlane.delete()
    # dq.execute()
    #
    # dq = Airline.delete()
    # dq.execute()
    #
    # dq = Marketing_Group_Airline.delete()
    # dq.execute()
    #
    # db.create_tables([Marketing_Group_Airline])
    # db.create_tables([Airline, AirPlane])

    # inst = Airline.select(
    #     Marketing_Group_Airline.iata_code_marketing_airline,
    #     Airline.airline,
    #     fn.COUNT(Airline.airline).alias('count')).join(Marketing_Group_Airline).group_by(Marketing_Group_Airline.iata_code_marketing_airline).dicts()
    # for o in inst:
    #     print(o)
    #
    # print('-----')
    #
    # inst = Airline.select(Airline.airline).where(Airline.airline == 'Southwest Airlines Co.')
    # print(len(inst))

    dq = AirPlane.select(AirPlane.tail_number).dicts()
    for o in dq:
        print(o)
