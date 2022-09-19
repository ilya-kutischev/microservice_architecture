import asyncio
import json
from time import sleep
import aiokafka
import pymongo
from fastapi import FastAPI, Body
import motor.motor_asyncio
from starlette import status
from starlette.responses import JSONResponse
from db_models import DataModel
from kafka_connector import produce_message, AsyncConsumer

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

    search = data.find_one({"datasearch": datasearch})
    # print(data.find())
    if search is None:
        datasearch = {  # creating
            "datasearch": datasearch,
            "count": 1
        }
        data.insert_one(datasearch)
        # print(data.find())
    else:
        # data.delete_one({"datasearch": datasearch})
        # print("here wee need to update instance")
        data.update_one({"datasearch": datasearch},{"$inc": {"count": 1}})
        # print(data.find())

    # Selecting instance
    # added_datasearch = data.find_one({})
    # print(f"Added New {added_datasearch}")
    return JSONResponse(status_code=status.HTTP_201_CREATED, content='')


config = {"bootstrap.servers": "localhost:9092"}

@app.on_event("startup")
async def startup_event():
    try:
        aio_consumer = AsyncConsumer()

        # await aio_consumer.consume()
        loop = asyncio.get_running_loop()
        loop.create_task(aio_consumer.consume())

    except (KeyboardInterrupt, SystemExit):
        print("Stats consumer FAILED")


# @app.on_event("shutdown")
# def shutdown_event():
#     aio_producer.close()
#     producer.close()




@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to our service"}


