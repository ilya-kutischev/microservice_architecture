from fastapi import FastAPI, Body, Depends
from elasticsearch import Elasticsearch


url = 'http://root:root@localhost:9200'
es = Elasticsearch(url)
es.create
app = FastAPI()



@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to our service"}