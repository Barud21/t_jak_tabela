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
        "INSERT INTO albums (Title, ArtistId) VALUES (?, ?)", (album.title, album.artist_id))
    await app.db_connection.commit()
    response.status_code = status.HTTP_201_CREATED
    return { "AlbumId": cursor.lastrowid, "Title": album.title, "ArtistId": album.artist_id }

@app.get("/albums/{album_id}")
async def get_new_album(response: Response, album_id: int):
    app.db_connection.row_factory = aiosqlite.Row
    cursor = await app.db_connection.execute(
        "SELECT * FROM albums WHERE AlbumId = :album_id", {"album_id": album_id})
    album = await cursor.fetchone()
    response.status_code = status.HTTP_200_OK
    return album


### ZADANIE 4 #########################################################
class Customer(BaseModel):
    company: str = None
    address: str = None
    city: str = None
    state: str = None
    country: str = None
    postalcode: str = None
    fax: str = None

@app.put("/customer/{customer_id}")
async def edit_customer(response: Response, customer_id: int, customer: Customer):
    cursor = await app.db_connection.execute(
        "SELECT CustomerId FROM customers WHERE CustomerId = :customer_id", {"customer_id": customer_id})
    client = await cursor.fetchone()
    if client is None:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"detail": {"error": "There is no such customer"}}
    update_data = customer.dict(exclude_unset=True)

    for key, value in update_data.items():
        # if value == None:
        #     continue
        key.capitalize()
        if key == "Postalcode":
            key = "PostalCode"
        cursor = await app.db_connection.execute(
            "UPDATE customers SET ? = ? WHERE CustomerId = ?", (key, value, customer_id)
        )
        await app.db_connection.commit()

    app.db_connection.row_factory = aiosqlite.Row
    cursor = await app.db_connection.execute(
        "SELECT * FROM customers WHERE CustomerId = ?", (customer_id, ))
    updated_client = await cursor.fetchone()
    return updated_client

