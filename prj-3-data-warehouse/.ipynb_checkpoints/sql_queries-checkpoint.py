import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
#
staging_events_table_create= ("""
CREATE TABLE staging_events(
   event_id INTEGER IDENTITY(0,1)
,  artist VARCHAR
,  auth VARCHAR
,  first_name VARCHAR
,  gender CHAR(1)
,  item_in_session SMALLINT
,  last_name VARCHAR
,  length FLOAT 
,  level VARCHAR
,  location VARCHAR
,  method VARCHAR
,  page VARCHAR
,  registration FLOAT
,  session_id INTEGER
,  song VARCHAR
,  status SMALLINT
,  ts TIMESTAMP
,  user_agent VARCHAR
,  user_id INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
  artist_id VARCHAR
, artist_latitude FLOAT
, artist_location VARCHAR
, artist_longitude FLOAT
, artist_name VARCHAR
, duration FLOAT
, num_songs SMALLINT
, song_id  VARCHAR
, title VARCHAR
, year SMALLINT
)
""")

songplay_table_create = ("""
CREATE TABLE songplays(
  songplay_id INTEGER NOT NULL PRIMARY KEY
, start_time TIMESTAMP NOT NULL REFERENCES time(start_time) SORTKEY DISTKEY
, user_id INTEGER NOT NULL REFERENCES users(user_id)
, level VARCHAR
, song_id VARCHAR NOT NULL REFERENCES songs(song_id)
, artist_id VARCHAR NOT NULL REFERENCES artists(artist_id)
, session_id INTEGER
, location VARCHAR
, user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE users( 
   user_id INTEGER NOT NULL PRIMARY KEY SORTKEY
, first_name VARCHAR
, last_name VARCHAR
, gender CHAR(1)
, level VARCHAR
)    
""")

song_table_create = ("""
CREATE TABLE songs(
  song_id INTEGER  PRIMARY KEY SORTKEY
, title VARCHAR
, artist_id VARCHAR
, year SMALLINT
, duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE artists(
  artist_id VARCHAR  PRIMARY KEY SORTKEY
, name VARCHAR
, location VARCHAR
, lattitude FLOAT
, longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE time(
  start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY DISTKEY
, hour SMALLINT NOT NULL
, day SMALLINT NOT NULL
, week SMALLINT NOT NULL
, month SMALLINT NOT NULL
, year SMALLINT NOT NULL
, weekday VARCHAR NOT NULL
)       
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events FROM {}
region {}
credentials {}
format as JSON {}
timeformat as 'epochmillisecs';
""").format(config['S3']['LOG_DATA'], config['S3']['REGION'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs FROM {}
region {}
credentials {}
format as JSON 'auto'; 
""").format(config['S3']['SONG_DATA'], config['S3']['REGION'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  DISTINCT 
  e.ts as start_time, 
, e.userId as user_id
, e.level
, s.song_id
, s.artist_id
, e.sessionId
, e.location
, e.userAgent as user_agent
FROM staging_events e
JOIN staging_songs  s
    ON e.song = s.title
    AND e.artist = s.artist_name
AND e.page == 'NextSong';
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
INSERT INTO time (
  start_time
, hour
, day
, week
, month
, year
, weekday
)
SELECT
  DISTINCT ts               AS start_time
, EXTRACT(hour FROM ts)       AS hour
, EXTRACT(day FROM ts)        AS day
, EXTRACT(week FROM ts)       AS week
, EXTRACT(month FROM ts)      AS month
, EXTRACT(year FROM ts)       AS year
, EXTRACT(dayofweek FROM ts)  as weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
