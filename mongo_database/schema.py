from mongoengine import connect, StringField, DynamicDocument, IntField
connect(
    db='plane',
    host='mongodb://test:test@localhost:27018/'
)


class Plane(DynamicDocument):
    name = StringField()
    mileage = IntField()


if __name__ == "__main__":
    Plane(name='test', mileage=11).save()

    for post in Plane.objects:
        print(post.name, post.mileage)
