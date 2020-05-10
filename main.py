import aiosqlite
import sqlite3
from pydantic import BaseModel
from fastapi import FastAPI, Response, status, Query

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


class CustomerUpdate(BaseModel):
    Company: str = None
    Address: str = None
    City: str = None
    State: str = None
    Country: str = None
    PostalCode: str = None
    Fax: str = None


class CustomerModel(BaseModel):
    CustomerId: int = None
    FirstName: str = None
    LastName: str = None
    Company: str = None
    Address: str = None
    City: str = None
    State: str = None
    Country: str = None
    PostalCode: str = None
    Phone: str = None
    Fax: str = None
    Email: str = None
    SupportRepId: int = None


@app.put("/customer/{customer_id}", response_model=CustomerModel)
async def edit_customer(response: Response, customer_id: int, customer: Customer):
    app.db_connection.row_factory = sqlite3.Row
    cursor = await app.db_connection.execute(
        "SELECT * FROM customers WHERE CustomerId = ?", (customer_id, ))
    data = await cursor.fetchone()
    if data is None:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"detail": {"error": "There is no such customer"}}

    stored_item_data = data
    stored_item_model = CustomerModel(**stored_item_data)
    data_update = CustomerUpdate(Company=customer.company, Address=customer.address, City=customer.city,
                                 State=customer.state, Country=customer.country, PostalCode=customer.postalcode,
                                 Fax=customer.fax)
    update_data = data_update.dict(exclude_unset=True)
    updated_customer = stored_item_model.copy(update=update_data)

    cursor = await app.db_connection.execute("UPDATE Company = ?, Address = ?, City = ?, State = ?, Country = ?, PostalCode "
                                       "= ?, Fax = ?", (updated_customer.Company, updated_customer.Address,
                                                        updated_customer.City, updated_customer.State,
                                                        updated_customer.Country, updated_customer.PostalCode,
                                                        updated_customer.Fax))
    await app.db_connection.commit()
    return updated_customer

# @app.put("/clients/{customer_id}")
# async def edit_client(response: Response, customer_id: int, customer: Customer):
#     cursor = await app.db_connection.execute(
#         "SELECT CustomerId FROM customers WHERE CustomerId = :customer_id", {"customer_id": customer_id})
#     data = await cursor.fetchone()
#     if data is None:
#         response.status_code = status.HTTP_404_NOT_FOUND
#         return {"detail": {"error": "There is no such customer"}}
#
#     update_data = customer.dict(exclude_unset=True)
#
#     for key, value in update_data.items():
#         # if value == None:
#         #     continue
#         key.capitalize()
#         if key == "Postalcode":
#             key = "PostalCode"
#         cursor = app.db_connection.execute("UPDATE customers SET ? = ? WHERE CustomerId = ? ", (key, value, customer_id))
#         app.db_connection.commit()
#
#     app.db_connection.row_factory = aiosqlite.Row
#     client = await app.db_connection.execute(
#         "SELECT * FROM customers WHERE CustomerId = ?", (customer_id, ))
#     updated_client = await client.fetchone()
#     return updated_client



### ZADANIE 5 #########################################################

@app.get("/sales")
async def sales_customer(response: Response, category: str = Query(None)):
    app.db_connection.row_factory = aiosqlite.Row

    if category == "customers":
        cursor = await app.db_connection.execute("SELECT invoices.CustomerId, Email, Phone, ROUND(SUM(Total), 2) AS Sum FROM customers INNER JOIN invoices ON customers.CustomerId = invoices.CustomerId GROUP BY invoices.CustomerId ORDER BY Sum DESC, invoices.CustomerId ASC")
        data = await cursor.fetchall()
        return data

    if category == "genres":
        cursor = await app.db_connection.execute("SELECT genres.Name, SUM(Quantity) AS Sum FROM invoice_items JOIN tracks ON invoice_items.TrackId = tracks.TrackId JOIN genres ON tracks.GenreId = genres.GenreId GROUP BY tracks.GenreId ORDER BY Sum DESC, genres.Name")
        data = await cursor.fetchall()
        return data
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"detail": {"error": "There is no such category"}}