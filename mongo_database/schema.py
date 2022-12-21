from mongoengine import connect, StringField, DynamicDocument, IntField
connect(
    db='plane',
    host='mongodb://test:test@localhost:27018/'
)


class Plane(DynamicDocument):
    name = StringField()
    mileage = IntField()


class Marketing_Group_Airline(DynamicDocument):
    marketing_airline_network = StringField()
    iata_code_marketing_airline = StringField()
    operated_or_branded_code_share_partners = StringField()
    dot_id_marketing_airline = IntField()


class Airline(DynamicDocument):
    airline = StringField()
    operating_airline = StringField()
    origin_state_name = StringField()
    dot_id_operating_airline = IntField()
    dot_id_marketing_airline = ForeignKeyField(Marketing_Group_Airline,
                                               backref='market_group',
                                               lazy_load=False)
                                               # to_field='dot_id_marketing_airline')


class Airplane(DynamicDocument):
    tail_number = StringField(null=True)
    # total_air_time = FloatField()
    dot_id_operating_airline = ForeignKeyField(Airline,
                                               backref='airline',
                                               lazy_load=False)


if __name__ == "__main__":
    Plane(name='test', mileage=11).save()

    for post in Plane.objects:
        print(post.name, post.mileage)
