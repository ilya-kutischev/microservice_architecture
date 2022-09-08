import asyncio
import json
from asyncio import sleep
import aiokafka
import pymongo
from fastapi import FastAPI, Body
import motor.motor_asyncio

from starlette import status
from starlette.responses import JSONResponse
from db_models import DataModel

app = FastAPI()


def add_datasearch(datasearch):
    # MONGO_DETAILS = "mongodb://mongodb/statistics"
    client = pymongo.MongoClient(
        host="mongodb",
        port=27017,
    )
    db = client["statistics"]
    data = db["data"] # collection
    print("STATS COLLECTION CREATED ========================================    ")

    print(data.find_one({"datasearch": datasearch})) # None

    search = data.find_one({"datasearch": datasearch})
    print(data.find())
    if search is None:
        datasearch = {  # creating
            "datasearch": datasearch,
            "count": 1
        }
        data.insert_one(datasearch)
        print(data.find())
    else:
        # data.delete_one({"datasearch": datasearch})
        print("here wee need to update instance")
        data.update_one({"datasearch": datasearch},{"$inc": {"count": 1}})
        print(data.find())

    # Selecting instance
    # added_datasearch = data.find_one({})
    # print(f"Added New {added_datasearch}")
    return JSONResponse(status_code=status.HTTP_201_CREATED, content='')


async def kafka_consumer(loop):
    # #KAFKA CONSUMER =====================================================
    # global consumer
    consumer = aiokafka.AIOKafkaConsumer(
        "my_topic",
        # loop=loop,
        bootstrap_servers='kafka:9092'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(
                "{}:{:d}:{:d}: key={} value={} timestamp_ms={}".format(
                    msg.topic, msg.partition, msg.offset, msg.key, msg.value,
                    msg.timestamp)
            )
            temp_dict = json.loads(msg.value)
            print(temp_dict)
            datasearch = list(temp_dict.keys())
            print(datasearch[0])
            add_datasearch(datasearch[0])

    finally:
        await consumer.stop()
        loop.create_task(kafka_consumer(loop))

loop = asyncio.get_running_loop()
loop.create_task(kafka_consumer(loop))


sleep(10)
add_datasearch("data")




# @app.on_event("startup")
# async def _startup_event():
    # await sleep(1)
    # MONGO_DETAILS = "mongodb://root:root@localhost/"
    # client = pymongo.MongoClient(MONGO_DETAILS)
    # db = client.statistics
    # data = db.data

    # asyncio.run(kafka_consumer())


@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to our service"}


