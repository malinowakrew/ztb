from peewee import *

db = MySQLDatabase('test', host='localhost', port=3307, user='test', password='test')


class BaseModel(Model):
    class Meta:
        database = db


class Plane(BaseModel):
    model = CharField()
    mileage = IntegerField()


if __name__ == "__main__":
    db.connect()
    db.create_tables([Plane])

    i = Plane(model='szybki', mileage=10)
    i.save()

    inst = Plane.select().where(Plane.mileage >= 10)
    for o in inst:
        print(o.model, o.mileage)
