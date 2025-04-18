from fastapi import FastAPI, Response
from .dbconn import DBConn
from sqlalchemy import text
import json
from contextlib import asynccontextmanager

db_conn = DBConn()

@asynccontextmanager
async def lifespan(app: FastAPI):
    with db_conn.get_connection() as connection:
        print("Connected to the database.")
        db_conn.populate_db()
        print("Database populated with SWIFT codes.")
        query = text('''
            SELECT "ADDRESS", "NAME", "COUNTRY ISO2 CODE", "COUNTRY NAME", "ISHQ", "SWIFT CODE", "HQ SWIFT CODE"
            FROM swift_codes LIMIT 1''')
        
        #hardcoding it is much faster than querying the database
        global branch_column_names 
        branch_column_names = ["address", "bankName", "countryISO2", "countryName", "isHeadquarter", "swiftCode"]
        #branch_column_names = connection.execute(query).keys()
        #for k in branch_column_names:
        #    new_keys = []
        #    for key in branch_column_names:
        #        match key:
        #            case "ADDRESS":
        #                new_keys.append("address")
        #            case "NAME":
        #                new_keys.append("bankName")
        #            case "COUNTRY ISO2 CODE":
        #                new_keys.append("countryISO2")
        #            case _:
        #                new_keys.append(key)
        #    branch_column_names = new_keys
    yield
    connection.close()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.get("/v1/swift_codes/{swift_code}")
async def read_swift_code(swift_code: str):
    with db_conn.get_connection() as connection:
        print(f"Received SWIFT code: {swift_code}")
        query = text('''
            SELECT "ADDRESS", "NAME", "COUNTRY ISO2 CODE", "COUNTRY NAME", "ISHQ", "SWIFT CODE", "HQ SWIFT CODE"
            FROM swift_codes WHERE "SWIFT CODE" = UPPER(:swiftcode)
            ''')
        result = connection.execute(query, {'swiftcode':swift_code})

        rows = [dict(zip(branch_column_names, row)) for row in result]
        if rows:
            return rows[0]
        else:
            return Response(status_code=404, content="SWIFT code not found.")