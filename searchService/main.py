from datetime import time

from fastapi import FastAPI, Body, Depends
from elasticsearch import Elasticsearch


app = FastAPI()


# @app.on_event("startup")
# def _startup_event():
#     time.sleep(10)
#     e1 = {
#         "first_name": "nitin",
#         "last_name": "panwar",
#         "age": 27,
#         "about": "Love to play cricket",
#         "interests": ['sports', 'music'],
#     }
#
#     # storing e1 document in Elasticsearch
#     es.index(index='ep1', id='1', document=e1)
#
#     # check data is in there, and structure in there
#     es.search(body={"query": {"match_all": {}}}, index='ep1')
#     es.indices.get_mapping(index='ep1')


@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to our service"}

@app.post("/add_to_db", tags=["add_to_db"])
def add_to_db():
    #connection

    url = 'http://root:root@0.0.0.0:9200'
    es = Elasticsearch(url)
    index_name = 'ep1'
    doctype = 'doc'
    es.indices.delete(index=index_name, ignore=[400, 404])
    es.indices.create(index=index_name, ignore=400)

    e1 = {
        "first_name": "nitin",
        "last_name": "panwar",
        "age": 27,
        "about": "Love to play cricket",
        "interests": ['sports', 'music'],
    }

    # storing e1 document in Elasticsearch
    es.index(index='ep1', id='1', document=e1)

    # check data is in there, and structure in there
    es.search(body={"query": {"match_all": {}}}, index='ep1')
    es.indices.get_mapping(index='ep1')