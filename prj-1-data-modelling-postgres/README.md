# Sparkify Song Play ETL

## Objective

To create a database to enable the Sparkify analytics team to easily query the JSON log and metadata files collected from their streaming music app. The key goal of this 
database is to support queries that will allow the analysts to understand the songs that their users are listening to.

## Source Datasets

### Song Metadata

Each file contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example:

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

Example file content:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log Dataset

The log files containing the users song plays are stored in the log_data directory partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

Example data:

```
{"artist":"N.E.R.D. FEATURING MALICE","auth":"Logged In","firstName":"Jayden","gender":"M","itemInSession":0,"lastName":"Fox","length":288.9922,"level":"free","location":"New Orleans-Metairie, LA","method":"PUT","page":"NextSong","registration":1541033612796.0,"sessionId":184,"song":"Am I High (Feat. Malice)","status":200,"ts":1541121934796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"101"}
```

Song plays are identified by filtering for page == "NextSong"


## Database schema

Sparkify's analysts are interesting in learning about the songs that their users listen to. This gives us a key "fact" (i.e. a song play) around which all queries will be based. They will need to perform adhoc queries involving aggregations (SQL will be a perfect tool for this), the data schema will not need to be flexible and the data volume is not so large as to require a distributed NoSQL DBMS such as Cassandra. Therefore we will design an OLAP data model within a PostgreSql database. It will have one main
fact table, with each record representing a song play and four dimension tables that, when joined upon the main fact table, will enrich the dataset with more detailed information about the artist, song, user and a time dimension to expand the timestamp (stored in milliseconds) into descriptive date parts to facilitate aggregations by standard intervals of time e.g. by month, by year etc.

### Fact table
Note: songplay_id uses the serial data type which is an autoincrementing integer. This means that postgreSQL will handle the population of this
field and we will not supply it in out insert statement for this table.

songplays:
    songplay_id serial PRIMARY KEY
    start_time timestamp REFERENCES time(start_time)
    user_id int REFERENCES users(user_id)
    level varchar
    song_id varchar REFERENCES songs(song_id)
    artist_id varchar REFERENCES artists(artist_id)
    session_id int
    location varchar
    user_agent varchar
    
### Dimension tables

users:
  user_id int PRIMARY KEY
  first_name varchar
  last_name varchar
  gender varchar
  level varchar
  
songs:
  song_id varchar PRIMARY KEY
  title varchar
  artist_id varchar
  year int
  duration decimal
  
artists:
  artist_id varchar PRIMARY KEY
  name varchar
  location varchar
  latitude float
  longitude float

time:
  start_time timestamp PRIMARY KEY
  hour int
  day int
  week int
  month int
  year int
  weekday varchar
  
  
## ETL pipeline

### Functions

#### process_data

A common function taking a cursor and connection for our postgreSQL database, a filepath and a file processing function. This will recursively "walk"
the given filepath for .JSON files and call the given file processing function for each file.

#### process_song_file

Takes path to a JSON file and a cursor. Using pandas read_json function it reads in the data as a data frame. Then for the artists and songs tables it selects the appropriate values as a python list which it supplies as parameters along with an sql INSERT statement template to the cursor to execute write data into the database.

#### process_log_file

Takes path to a JSON file and a cursor. Using pandas read_json function it reads in the data as a data frame selecting only those rows where page==NextSong. 

The timestamp field (stored in milliseconds) is then split into the necessary date parts using pandas dt attribute and written to the time dimension table. 

User details are selected from the data frame and written to the users dimension. 

To load the main fact table, the data frame is iterated over and for each record an artist_id and song_id are looked via a select query (joining artist and song tables) which filters by song,artist and length. If the lookup fails to find a match artist_id and song_id are set to None. artist_id, song_id and the remaining attributes needed for the fact table are assembled and supplied as parameters along with an INSERT statement template which is execute to write the data to the database.

#### main
Makes a connection to the database and creates a cursor. 

Call process data for song and log files passing in the appropriate paths.

Closes the connection.

#### Future enhancements
Performance is perfectly adequate with the current amount of data but one can see with larger numbers of files this may become an issue. At present each insert is performed individually and thus each requires a round trip to the database. A possible improvement would be to batch together multiple rows for a particular target table and then make use of execute_batch [psycopg extras](http://initd.org/psycopg/docs/extras.html) which should lead to much improved write performance.


## Project structure

.\data\song_data - song metadata json files
.\data\long_data - log data json files
.\create_tables.py - python script to drop/create the database then create the necessary tables as defined in sql_queries.py
.\etl.py - the etl pipeline script contents of which is defined above in [ETL pipeline](## ETL pipeline)

.\insert_sql_gen.ipynb - python notebook to generate insert statements from table metadata within the postgreSQL database. This saved a bit of time as opposed to manually writing the initial inserts and allows the structure of the statement to be changed all at one. If, for example, you wanted to change the dimensions to be SCD's (slowly changing dimensions) it would be simple to modify this generator to output the necessary statements.

.\sql_queries.py - python script storing sql queries using by create_tables.py to create the database and the various queries used by etl.py
.\test.ipynb - Some quick test queries to show that the etl is working as intended to populate the database tables

## Example queries
Please view .\example_queries.ipynb to see some basic examples of queries the analysts could use.