from peewee import *

db = MySQLDatabase('test', host='localhost', port=3309, user='test', password='test')


class BaseModel(Model):
    class Meta:
        database = db


class Plane(BaseModel):
    model = CharField()
    mileage = IntegerField()


if __name__ == "__main__":
    # db.connect()
    # db.create_tables([Plane])

    i = Plane(model='szybki_ale_stary', mileage=4800000)
    i.save()

    inst = Plane.select().where(Plane.mileage > 1000)
    for o in inst:
        print(o.model, o.mileage)
