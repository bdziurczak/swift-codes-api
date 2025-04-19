from fastapi import FastAPI, Response
from .dbconn import DBConn
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import HTTPException

db_conn = DBConn()

#populating database is done during each startup 
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
@app.post("/v1/swift-codes")
async def add_swift_code(data: dict):
        #Country Name data is ommitted because it is not needed to retrieve country name for given entity from db
        #still required because of request structure guidelines
        required_fields: set[str] = {"address", "bankName", "countryISO2", "countryName", "isHeadquarter", "swiftCode"}
        if not required_fields.issubset(data.keys()):
            raise HTTPException(status_code=400, detail="Missing required fields in the request body.")
        message = await db_conn.add_swift_code_entry(data, required_fields)
        return message   
    
@app.delete("/v1/swift-codes/{swift_code}")
async def delete_swift_code(swift_code: str):
    deleted = await db_conn.delete_swift_code_entry(swift_code)
    if deleted:
        return {"message": "SWIFT code deleted successfully."}
    else:
        raise HTTPException(status_code=404, detail="SWIFT code not found.")