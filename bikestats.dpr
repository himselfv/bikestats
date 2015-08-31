program bikestats;

{$R *.res}

uses
  SysUtils, Classes,
  Db, sqlite3, sqlite3ds,
  IdHttp,
  superobject;

const
  sQueryAddr: string = 'http://velobike.ru/proxy/parkings/';

var
  Db: TSQLiteDB;

procedure InitDb;
var tables: TDataset;
  foundStations, foundSamples: boolean;
begin
  Db := TSqliteDb.Create(ChangeFileExt(paramstr(0),'.db'));
  tables := Db.Query('SELECT name FROM sqlite_master WHERE type = "table"');
  foundStations := false;
  foundSamples := false;
  while not tables.Eof do begin
    foundStations := foundStations or SameText(tables.Fields[0].Value, 'Stations');
    foundSamples := foundSamples or SameText(tables.Fields[0].Value, 'Samples');
    tables.Next;
  end;

  if not foundStations then
    Db.Exec('CREATE TABLE Stations (Id INTEGER PRIMARY KEY ASC, Address TEXT, lat REAL, lon REAL)');
  if not foundSamples then
    Db.Exec('CREATE TABLE Samples (StationId INTEGER, Timestamp INTEGER, TotalPlaces INTEGER, FreePlaces INTEGER)');
end;

procedure FreeDb();
begin
  FreeAndNil(Db);
end;

function HttpGet(const url: string): string;
var http: TIdHttp;
  response: TMemoryStream;
begin
  response := nil;
  http := TIdHttp.Create(nil);
  try
    response := TMemoryStream.Create;
    http.Get(url, response);
    SetString(Result, PAnsiChar(response.Memory), response.Size);
  finally
    FreeAndNil(http);
    FreeAndNil(response);
  end;

end;

type
  UnixTime = int64;

const
 //UnixStartDate: the TDatetime of 01/01/1970
  UnixStartDate: TDateTime = 25569.0;
  SecondsPerDay = 86400;
  OneSecond = 1 / SecondsPerDay;
  MillisecondsPerSecond = 1000;
  MillisecondsPerDay = SecondsPerDay * MillisecondsPerSecond;
  OneMillisecond = 1 / MillisecondsPerDay;

function DatetimeToUnixtime(const ADatetime: TDatetime): UnixTime;
begin
  Result := Round((ADatetime - UnixStartDate) * MillisecondsPerDay);
end;

function UnixtimeToDatetime(const AUnixtime: UnixTime): TDatetime;
begin
  Result := UnixStartDate + (AUnixtime / MillisecondsPerDay);
end;



procedure ParseItem(tm: UnixTime; item: ISuperObject);
var id: integer;
begin
  id := item.I['Id'];
  if Db.Query('SELECT * FROM Stations WHERE Id='+IntToStr(id)).IsEmpty then
    Db.Exec('INSERT INTO Stations (Id, Address, lat, lon) VALUES ('
      +IntToStr(id)+', "'+item.S['Address']+'", '+item.O['Position'].S['Lat']+', '+item.O['Position'].S['Lon']+')'
    );
  Db.Exec('INSERT INTO Samples (StationId, Timestamp, TotalPlaces, FreePlaces) VALUES ('
     +IntToStr(id)+', '+IntToStr(tm)+', '+item.S['TotalPlaces']+', '+item.S['FreePlaces']
  +')');

end;

procedure Query();
var text: string;
  items: TSuperArray;
  i: integer;
  tm: UnixTime;
begin
  tm := DatetimeToUnixTime(now());
  text := HttpGet(sQueryAddr);
  items := SO(text).A['Items'];
  for i := 0 to items.Length-1 do
    ParseItem(tm, items.O[i]);
end;

begin
  try
    InitDb();
    Query();
    FreeDb();
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
