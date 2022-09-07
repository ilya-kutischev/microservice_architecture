import asyncio
from asyncio import sleep
import aiokafka
from fastapi import FastAPI, Body
import motor.motor_asyncio
from starlette import status
from starlette.responses import JSONResponse
from db_models import DataModel

app = FastAPI()


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
    finally:
        await consumer.stop()


@app.on_event("startup")
async def _startup_event():
    await sleep(15)
    MONGO_DETAILS = "mongodb://127.0.0.1:27017"
    client = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)
    global db
    db = client.stats
    stats_collection = db.get_collection("statistics")
    # print(stats_collection)

    # asyncio.run(kafka_consumer())
    loop = asyncio.get_running_loop()
    loop.create_task(kafka_consumer(loop))

@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to our service"}


@app.post("/add_stats", response_description="Add new student")
async def add_datasearch(student: DataModel = Body(...)):
    datasearch = await db["statistics"].insert_one(student)

    # Selecting instance
    created_student = await db["statistics"].find_one({"_id": datasearch.inserted_id})
    return JSONResponse(status_code=status.HTTP_201_CREATED, content=created_student)