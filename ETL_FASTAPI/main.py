from fastapi import FastAPI
from pydantic import BaseModel
from typing import List,Optional

app=FastAPI(title='Local stateless pipeline API')

DB_MOCK=[]

class DataPayload(BaseModel):
    id:Optional[int]=None
    name:Optional[str]=None
    value:Optional[float]=None

@app.get('/')
def read_root():
    return {"status":"running","storage":"in-memory"}

@app.post("/data/batch")
def recieve_bath(payload: List[DataPayload]):
    global DB_MOCK
    DB_MOCK.extend(payload)
    return {"status":"success","record_inserted":len(payload)}

@app.get("/data")
def get_data():
    return {"count":len(DB_MOCK),"records":DB_MOCK}
