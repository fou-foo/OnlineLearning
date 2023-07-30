import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

bucket_log=config.get('S3', 'LOG_DATA')
bucket_song=config.get('S3', 'SONG_DATA')

role=config.get('IAM_ROLE', 'ARN')
json_format=config.get('S3', 'LOG_JSONPATH')

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = """
CREATE SCHEMA IF NOT EXISTS sparkifyDWH;
SET search_path TO sparkifyDWH;
"""
# DROP TABLE IF EXISTS events_stag  error naming tables 
staging_songs_table_drop = " DROP TABLE IF EXISTS staging_songs ;"
staging_events_table_drop = " DROP TABLE IF EXISTS songs_stag; "
songplay_table_drop = "DROP TABLE IF EXISTS  songplays ;"
user_table_drop = "DROP TABLE IF EXISTS users  ;"
song_table_drop = "DROP TABLE IF EXISTS   songs ;"
artist_table_drop = "DROP TABLE IF EXISTS  artists ;"
time_table_drop = "DROP TABLE IF EXISTS   time ;"


# CREATE TABLES

staging_events_table_create= ("""
SET search_path TO sparkifyDWH;
CREATE TABLE IF NOT EXISTS staging_events (
       artist         TEXT,
       auth           TEXT,
       firstName      TEXT,
       gender         TEXT,
       itemInSession  INTEGER,
       lastName       TEXT,
       length         FLOAT8,
       level          TEXT,
       location       TEXT,
       method         TEXT,
       page           TEXT,
       registration   BIGINT,
       sessionId      INTEGER,
       song           TEXT,
       status         INTEGER,
       ts             TIMESTAMP,
       userAgent      TEXT,
       userId         INTEGER);
    
""")

staging_songs_table_create = ("""
SET search_path TO sparkifyDWH;
CREATE TABLE IF NOT EXISTS songs_stag (
        artist_id        TEXT,
        artist_latitude  FLOAT8,
        artist_location  TEXT,
        artist_longitude FLOAT8,
        artist_name      TEXT,
        duration         FLOAT8,
        num_songs        INTEGER,
        song_id          TEXT,
        title            TEXT,
        year             INTEGER);
""")
# {"artist_id":"AR73AIO1187B9AD57B","artist_latitude":37.77916,"artist_location":"San Francisco, CA","artist_longitude":-122.42005,"artist_name":"Western Addiction","duration":118.07302,"num_songs":1,"song_id":"SOQPWCR12A6D4FB2A3","title":"A Poor Recipe For Civic Cohesion","year":2005}
#{"artist_id":"ARSVTNL1187B992A91","artist_latitude":51.50632,"artist_location":"London, England","artist_longitude":-0.12714,"artist_name":"Jonathan King","duration":129.85424,"num_songs":1,"song_id":"SOEKAZG12AB018837E","title":"I'll Slap Your Face (Entertainment USA Theme)","year":2001}


songplay_table_create = ("""
SET search_path TO sparkifyDWH;
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id integer IDENTITY(0,1) sortkey,
    start_time TIMESTAMP NOT NULL ,
    user_id INTEGER NOT NULL  ,
    level TEXT NOT NULL, 
    song_id TEXT  , 
    artist_id TEXT   ,
    session_id INTEGER ,
    location TEXT, 
    user_agent TEXT );

""")

user_table_create = ("""
SET search_path TO sparkifyDWH;
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER  not null sortkey,
    first_name VARCHAR(40) NOT NULL, 
    last_name VARCHAR(40) NOT NULL, 
    gender VARCHAR(1) NOT NULL, 
    level VARCHAR(5) NOT NULL ) 
    diststyle all;

""")

song_table_create = ("""
SET search_path TO sparkifyDWH;
CREATE TABLE IF NOT EXISTS songs ( 
    song_id TEXT not null sortkey, 
    title TEXT NOT NULL, 
    artist_id TEXT NOT NULL, 
    year INTEGER NOT NULL, 
    duration FLOAT8 )
    diststyle all;
""")

artist_table_create = ("""
SET search_path TO sparkifyDWH;
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT null sortkey, 
    name TEXT NOT NULL, 
    location TEXT , 
    latitude FLOAT8 , 
    longitude FLOAT8    )
     diststyle all;

""")

time_table_create = ("""
SET search_path TO sparkifyDWH;
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP null sortkey, 
    hour INTEGER NOT NULL, 
    day INTEGER NOT NULL, 
    week INTEGER NOT NULL,
    month INTEGER NOT NULL, 
    year INTEGER NOT NULL, 
    weekday VARCHAR(10))
     diststyle all;
""")

# STAGING TABLES

staging_events_copy = """
SET search_path TO sparkifyDWH;
copy staging_events from {} 
credentials 'aws_iam_role={}'
json {}
timeformat as 'epochmillisecs';
""".format(bucket_log, role, json_format)

staging_songs_copy = ("""
SET search_path TO sparkifyDWH;
COPY songs_stag FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON 'auto' 
""").format( bucket_song, role )

# FINAL TABLES

songplay_table_insert = ("""
SET search_path TO sparkifyDWH;
INSERT INTO sparkifyDWH.songplays 

SELECT se.ts, se.userId , se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
       
    FROM sparkifyDWH.staging_events AS se
    LEFT JOIN sparkifyDWH.songs_stag AS ss
    ON se.song=ss.title
;
""")

user_table_insert = ("""
SET search_path TO sparkifyDWH;
INSERT INTO sparkifyDWH.users

SELECT DISTINCT userId, firstName, lastName,  gender , level
    FROM sparkifyDWH.staging_events
""")

song_table_insert = ("""
SET search_path TO sparkifyDWH;
INSERT INTO sparkifyDWH.songs

SELECT DISTINCT song_id, title, artist_id, year, duration 
    FROM sparkifyDWH.songs_stag;
""")

artist_table_insert = ("""
SET search_path TO sparkifyDWH;
INSERT INTO sparkifyDWH.artists

SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude,  artist_longitude
    FROM sparkifyDWH.songs_stag;
""")
#{"artist_id":"AR73AIO1187B9AD57B","artist_latitude":37.77916,"artist_location":"San Francisco, CA","artist_longitude":-122.42005,"artist_name":"Western Addiction","duration":118.07302,"num_songs":1,"song_id":"SOQPWCR12A6D4FB2A3","title":"A Poor Recipe For Civic Cohesion","year":2005}


time_table_insert = ("""
SET search_path TO sparkifyDWH;
INSERT INTO sparkifyDWH.time

SELECT DISTINT ts, EXTRACT ( HOUR FROM current_timestamp()), EXTRACT ( DAY FROM current_timestamp()), EXTRACT ( WEEK FROM current_timestamp()), 
                EXTRACT ( MONTH FROM current_timestamp()), EXTRACT ( YEAR FROM current_timestamp()), EXTRACT ( DAYOFWEEK FROM current_timestamp())
    FROM sparkifyDWH.staging_events;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
