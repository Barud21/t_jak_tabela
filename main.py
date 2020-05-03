import aiosqlite
import sqlite3
from pydantic import BaseModel
from fastapi import FastAPI

app = FastAPI()

@app.on_event("startup")
async def startup():
    app.db_connection = await aiosqlite.connect('chinook.db')

@app.on_event("shutdown")
async def shutdown():
    await app.db_connection.close()

@app.get("/data")
async def root():
    cursor = await app.db_connection.execute("SELECT * FROM tracks")
    data = await cursor.fetchall()
    return {"data": data}


@app.get("/tracks")
async def get_tracks(page: int = 0, per_page: int = 10):
    app.db_connection.row_factory = aiosqlite.Row
    cursor = await app.db_connection.execute(
        "SELECT * FROM tracks ORDER BY TrackId LIMIT :per_page OFFSET :per_page*:page",
        {'page': page, 'per_page': per_page})
    tracks = await cursor.fetchall()
    return tracks
