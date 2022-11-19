from pymongo import MongoClient
import asyncio
from beanie import Document, init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReadPreference


class Plane(Document):
    model: str
    mileage: int


async def init():
    client = AsyncIOMotorClient("mongodb://test:test@localhost:27019/")
    await init_beanie(database=client.get_database('test', read_preference=ReadPreference.SECONDARY),
                      document_models=[Plane])


async def plane_add():
    await init()
    plane = Plane(model='szybki_billek', mileage=100)
    await plane.insert()

if __name__ == "__main__":
    asyncio.run(plane_add())
