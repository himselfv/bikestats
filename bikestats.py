import os
import sqlite3
import urllib2
import json
import time

query_addr = "http://velobike.ru/proxy/parkings/"

def init_db():
    (fname, ext) = os.path.splitext(__file__)
    db = sqlite3.connect(fname + ".db")
    db.row_factory = sqlite3.Row
    tables = [row['name'] for row in db.execute('SELECT name FROM sqlite_master WHERE type = "table"')]
    if not 'Stations' in tables:
        db.execute('CREATE TABLE Stations (Id INTEGER PRIMARY KEY ASC, Address TEXT, lat REAL, lon REAL)')
    if not 'Samples' in tables:
        db.execute('CREATE TABLE Samples (StationId INTEGER, Timestamp INTEGER, TotalPlaces INTEGER, FreePlaces INTEGER)')
    return db

db = init_db()
resp_text = urllib2.urlopen(query_addr).read()
tm = int(time.time() * 1000) # unix timestamp * 1000 + mseconds, as int
items = json.loads(resp_text)['Items']
for item in items:
    id = item['Id']
    if next(db.execute('SELECT * FROM Stations WHERE Id=?', [id]), None) is None:
        db.execute('INSERT INTO Stations (Id, Address, lat, lon) VALUES (?, ?, ?, ?)',
            [id, item['Address'], item['Position']['Lat'], item['Position']['Lon']
            ])
    db.execute('INSERT INTO Samples (StationId, Timestamp, TotalPlaces, FreePlaces) VALUES (?, ?, ?, ?)',
        [id, tm, item['TotalPlaces'], item['FreePlaces']])

db.commit()
db.close()