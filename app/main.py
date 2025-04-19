from fastapi import FastAPI, Response
from .dbconn import DBConn
from contextlib import asynccontextmanager
from pathlib import Path

db_conn = DBConn()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Connected to the database.")
    db_conn.populate_db()
    print("Database populated with SWIFT codes.")
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.get("/v1/swift-codes/{swift_code}")
async def read_swift_code(swift_code: str):
    res = await db_conn.get_data_by_swift_code(swift_code)
    if res:
        return res
    else:
        return Response(status_code=404, content="SWIFT code not found.")
    
@app.get("/v1/swift-codes/country/{countryISO2code}")
async def read_swift_code_by_country(countryISO2code: str):
    res = await db_conn.get_data_by_country(countryISO2code)
    if res:
        return res
    else:
        return Response(status_code=404, content="SWIFT code not found.")
