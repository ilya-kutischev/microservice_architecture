import asyncio
import json
from asyncio import sleep
import aiokafka
from confluent_kafka import KafkaException
from fastapi import FastAPI, Body, Depends
from elasticsearch import Elasticsearch
from starlette.exceptions import HTTPException

from kafka_connector import AsyncConsumer, produce_message

app = FastAPI()
config = {"bootstrap.servers": "localhost:9092"}


@app.on_event("startup")
async def _startup_event():
    await sleep(15)
    url = 'http://elasticsearch:9200'
    es = Elasticsearch(url)
    index_name = 'ep1'
    es.indices.delete(index=index_name, ignore=[400, 404])
    es.indices.create(index=index_name, ignore=400)

    # global producer, aio_producer, aio_consumer
    # aio_producer = AIOProducer(config)
    # producer = Producer(config)
    aio_consumer = AsyncConsumer()

    await aio_consumer.consume()
    # loop = asyncio.get_running_loop()
    # loop.create_task(aio_consumer.consume())



@app.on_event("shutdown")
def shutdown_event():
    pass
    # aio_producer.close()
    # producer.close()


@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to our service"}


@app.post("/add_to_db", tags=["add_to_db"])
def add_to_db(data):
    #connection
    url = 'http://elasticsearch:9200'
    es = Elasticsearch(url)
    index_name = 'ep1'
    e1 = data
    # e1 = {
    #     "header": "My Header",
    #     "data": "Love to play cricket",
    # }
    es.index(index=index_name, document=e1)


# async def kafka_producer(loop, query):
#     # KAFKA CONNECTION ==========================================================
#     producer = aiokafka.AIOKafkaProducer(loop=loop, bootstrap_servers='kafka:9092')
#     await producer.start()
#     try:
#         await producer.send_and_wait("my_topic", query)
#     finally:
#         await producer.stop()

@app.get("/get_instance", tags=["get_instance"])
async def get_instance():
    url = 'http://elasticsearch:9200'
    es = Elasticsearch(url)
    index_name = 'ep1'
    query = {"match_all":{}}
    results = es.search(index=index_name, query=query)

    # asyncio.run(kafka_consumer())
    # loop = asyncio.get_running_loop()
    # loop.create_task(kafka_producer(loop, json.dumps(query).encode('utf-8')))
    try:
        result = await produce_message("search_stats", json.dumps(query).encode('utf-8'))
        return {"timestamp": result.timestamp()}
    except KafkaException as ex:
        raise HTTPException(status_code=500, detail=ex.args[0].str())
    print(f"Info: {results}")

