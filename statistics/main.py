import asyncio
from asyncio import sleep
import aiokafka
from fastapi import FastAPI, Body
import motor.motor_asyncio
from starlette import status
from starlette.responses import JSONResponse
from db_models import DataModel

app = FastAPI()


@app.post("/add_stats", response_description="Add new student")
async def add_datasearch(datasearch):
    MONGO_DETAILS = "mongodb://root:root@mongodb:27017"

    client = motor.motor_asyncio.AsyncIOMotorClient(
        io_loop=asyncio.get_running_loop(),
        host="mongodb",
        port=27017,
        username="root",
        password="root",
    )

    db = client.statistics

    stats_collection = db.get_collection("statistics")
    datasearch = await db["statistics"].insert_one(datasearch)
    # Selecting instance
    added_datasearch = await db["statistics"].find_one({"_id": datasearch.inserted_id})
    print(f"Added New {added_datasearch}")
    return JSONResponse(status_code=status.HTTP_201_CREATED, content=added_datasearch)

async def kafka_consumer(loop):
    # #KAFKA CONSUMER =====================================================
    # global consumer
    consumer = aiokafka.AIOKafkaConsumer(
        "my_topic",
        loop=loop,
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
            datasearch ={
                "datasearch": msg.value,
                "count": "1"
            }

            loop = asyncio.get_running_loop()
            loop.create_task(add_datasearch(datasearch))
    finally:
        await consumer.stop()


@app.on_event("startup")
async def _startup_event():
    await sleep(15)
    MONGO_DETAILS = "mongodb://root:root@mongodb:27017"

    client = motor.motor_asyncio.AsyncIOMotorClient(
        host="mongodb",
        port=27017,
        username="root",
        password="root",
        authSource="admin"
    )

    global db
    db = client.statistics
    stats_collection = db.get_collection("statistics")
    # print(stats_collection)

    # asyncio.run(kafka_consumer())
    loop = asyncio.get_running_loop()
    loop.create_task(kafka_consumer(loop))

@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to our service"}


