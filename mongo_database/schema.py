from mongoengine import connect, StringField, DynamicDocument, IntField
from mongoengine import CASCADE, Document, StringField, IntField, ListField, DateTimeField, ReferenceField

connect(
    db='test',
    host='mongodb://test:test@localhost:27018/'
)


class Plane(DynamicDocument):
    name = StringField()
    mileage = IntField()


class Marketing_Group_Airline(DynamicDocument):
    marketing_airline_network = StringField()
    iata_code_marketing_airline = StringField()
    # operated_or_branded_code_share_partners = StringField()
    dot_id_marketing_airline = IntField()


class Airline(DynamicDocument):
    airline = StringField()
    operating_airline = StringField()
    # origin_state_name = StringField()
    dot_id_operating_airline = IntField()
    # operated_or_branded_code_share_partners = ListField(ReferenceField(Marketing_Group_Airline, reverse_delete_rule=CASCADE))
    dot_id_marketing_airline = ListField(ReferenceField(Marketing_Group_Airline, reverse_delete_rule=CASCADE))


class Airplane(DynamicDocument):
    tail_number = StringField(null=True)
    # total_air_time = FloatField()
    dot_id_operating_airline = ListField(ReferenceField(Airline, reverse_delete_rule=CASCADE))


if __name__ == "__main__":
    pass
    # Plane(name='test', mileage=11).save()
    #
    # for post in Plane.objects:
    #     print(post.name, post.mileage)
