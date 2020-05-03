import aiosqlite
import sqlite3
from pydantic import BaseModel
from fastapi import FastAPI, Response, status

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


### ZADANIE 1 #########################################################
@app.get("/tracks")
async def get_tracks(page: int = 0, per_page: int = 10):
    app.db_connection.row_factory = aiosqlite.Row
    cursor = await app.db_connection.execute(
        "SELECT * FROM tracks ORDER BY TrackId LIMIT :per_page OFFSET :per_page*:page",
        {'page': page, 'per_page': per_page})
    tracks = await cursor.fetchall()
    return tracks


### ZADANIE 2 #########################################################
@app.get("/tracks/composers")
async def composer_tracks(response: Response, composer_name: str):
    app.db_connection.row_factory = lambda cursor, x: x[0]
    cursor = await app.db_connection.execute(
        "SELECT Name FROM tracks WHERE Composer = :composer_name ORDER BY Name",
        {'composer_name': composer_name})
    composer = await cursor.fetchall()
    if len(composer) == 0:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"detail": {"error": "There is no such composer in database"}}
    return composer


### ZADANIE 3 #########################################################
class Albums(BaseModel):
    title: str
    artist_id: int

@app.post("/albums")
async def add_new_album(response: Response, album: Albums):
    cursor = await app.db_connection.execute(
        "SELECT ArtistId FROM artists WHERE ArtistId = :artist_id", {"artist_id": album.artist_id})
    artist = await cursor.fetchone()
    if artist is None:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"detail": {"error": "There is no such composer in database"}}
    cursor = await app.db_connection.execute(
        "INSERT INTO albums (Title, ArtistId) VALUES (?, ?)", (album.title, album.artist_id)
    )
    await app.db_connection.commit()
    response.status_code = status.HTTP_201_CREATED
    new_album = {"AlbumId": cursor.lastrowid+1, "Title": album.title, "ArtistId": album.artist_id}
    return new_album
