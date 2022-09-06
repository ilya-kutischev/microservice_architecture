from asyncio import sleep

from fastapi import FastAPI, Body, Depends
from elasticsearch import Elasticsearch


app = FastAPI()


@app.on_event("startup")
async def _startup_event():
    await sleep(15)
    url = 'http://root:root@elasticsearch:9200'
    es = Elasticsearch(url)
    index_name = 'ep1'
    doctype = 'doc'
    es.indices.delete(index=index_name, ignore=[400, 404])
    es.indices.create(index=index_name, ignore=400)


@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to our service"}


@app.post("/add_to_db", tags=["add_to_db"])
def add_to_db():
    #connection
    url = 'http://root:root@elasticsearch:9200'
    es = Elasticsearch(url)
    index_name = 'ep1'
    # doctype = 'doc'
    # es.indices.delete(index=index_name, ignore=[400, 404])
    # es.indices.create(index=index_name, ignore=400)

    e1 = {
        "header": "My Header",
        "data": "Love to play cricket",
    }

    # storing e1 document in Elasticsearch
    es.index(index='ep1', id='1', document=e1)


@app.get("/get_instance", tags=["get_instance"])
def get_instance():
    url = 'http://root:root@elasticsearch:9200'
    es = Elasticsearch(url)
    index_name = 'ep1'
    # query = {
    #     "query": {
    #         "bool": {
    #             "must": {
    #                 "term": {
    #                     "header": "My Header"
    #                 }
    #             }
    #         }
    #     }
    # }
    query = {"match_all":{}}
    results = es.search(index=index_name, query=query)
    print(results)
    return results

