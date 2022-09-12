import asyncio
import json
from asyncio import sleep
import aiokafka
from fastapi import FastAPI, Body, Depends
from elasticsearch import Elasticsearch


app = FastAPI()


@app.on_event("startup")
async def _startup_event():
    await sleep(15)
    url = 'http://elasticsearch:9200'
    es = Elasticsearch(url)
    index_name = 'ep1'
    es.indices.delete(index=index_name, ignore=[400, 404])
    es.indices.create(index=index_name, ignore=400)


@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to our service"}


@app.post("/add_to_db", tags=["add_to_db"])
def add_to_db():
    #connection
    url = 'http://elasticsearch:9200'
    es = Elasticsearch(url)
    index_name = 'ep1'
    e1 = {
        "header": "My Header",
        "data": "Love to play cricket",
    }
    es.index(index=index_name, id='1', document=e1)


async def kafka_producer(loop, query):
    # KAFKA CONNECTION ==========================================================
    producer = aiokafka.AIOKafkaProducer(loop=loop, bootstrap_servers='kafka:9092')
    await producer.start()
    try:
        await producer.send_and_wait("my_topic", query)
    finally:
        await producer.stop()

@app.get("/get_instance", tags=["get_instance"])
async def get_instance():
    url = 'http://elasticsearch:9200'
    es = Elasticsearch(url)
    index_name = 'ep1'
    query = {"match_all":{}}
    results = es.search(index=index_name, query=query)

    # asyncio.run(kafka_consumer())
    loop = asyncio.get_running_loop()
    loop.create_task(kafka_producer(loop, json.dumps(query).encode('utf-8')))

    print(f"Info: {results}")

