from fastapi import FastAPI, Response
from app.swift_code_parser import SwiftCodeParser

app = FastAPI()

parser = SwiftCodeParser()

@app.get("/")
async def read_root():
    df = await parser.get_df()
    return Response(content=df.to_json(orient="records"), media_type="application/json")
