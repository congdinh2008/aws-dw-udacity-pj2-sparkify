import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP SCHEMAS

final_schema_drop = "DROP SCHEMA IF EXISTS final CASCADE"
staging_schema_drop = "DROP SCHEMA IF EXISTS staging CASCADE"

# CREATE SCHEMAS

final_schema_create = "CREATE SCHEMA IF NOT EXISTS final"
staging_schema_create = "CREATE SCHEMA IF NOT EXISTS staging"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging.events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging.songs;"
songplay_table_drop = "DROP TABLE IF EXISTS final.songplay;"
user_table_drop = "DROP TABLE IF EXISTS final.user;"
song_table_drop = "DROP TABLE IF EXISTS final.song;"
artist_table_drop = "DROP TABLE IF EXISTS final.artist;"
time_table_drop = "DROP TABLE IF EXISTS final.time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging.events(
    artist VARCHAR(255),
    auth VARCHAR(255),
    firstName VARCHAR(255),
    gender VARCHAR(255),
    itemInSession INT,
    lastName VARCHAR(255),
    length FLOAT,
    level VARCHAR(255),
    location VARCHAR(255),
    method VARCHAR(255),
    page VARCHAR(255),
    registration FLOAT,
    sessionId INT,
    song VARCHAR(255),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(255),
    userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging.songs(
 num_songs INT,
 artist_id VARCHAR(255),
 artist_latitude FLOAT,
 artist_longitude FLOAT,
 artist_location VARCHAR(255),
 artist_name VARCHAR(255),
 song_id VARCHAR(255),
 title VARCHAR(255),
 duration FLOAT,
 year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS final.songplay(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY DISTKEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id INT,
    level VARCHAR(255),
    song_id VARCHAR(255),
    artist_id VARCHAR(255),
    session_id INT,
    location VARCHAR(255),
    user_agent VARCHAR(255)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS final.users(
    user_id INT PRIMARY KEY SORTKEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(255),
    level VARCHAR(255)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS final.songs(
    song_id VARCHAR(255) PRIMARY KEY SORTKEY,
    title VARCHAR(255),
    artist_id VARCHAR(255),
    year INT,
    duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS final.artists(
    artist_id VARCHAR(255) PRIMARY KEY SORTKEY,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS final.time(
    start_time TIMESTAMP PRIMARY KEY NOT NULL SORTKEY,
    hour INT,
    day INT,
    week INT,
    weekday VARCHAR(255),
    month INT,
    year INT
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging.events FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
FORMAT AS JSON {};
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'IAM_ROLE_ARN'),
            config.get('CLUSTER', 'DWH_REGION'),
            config.get('S3', 'LOG_JSONPATH')
            )

staging_songs_copy = ("""
COPY staging.songs FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
FORMAT AS JSON 'auto';
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'IAM_ROLE_ARN'),
            config.get('CLUSTER', 'DWH_REGION')
            )

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO final.songplay(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT DISTINCT 
    timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId ,
    se.location,
    se.userAgent
FROM staging.songs AS ss
JOIN staging.events AS se
ON ss.title = se.song AND ss.artist_name = se.artist
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO final.users(
    user_id,
    first_name,
    last_name,
    gender,
    level)
SELECT DISTINCT
    se.userId,
    se.firstName,
    se.lastName,
    se.gender,
    se.level
FROM staging.events AS se
WHERE se.userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO final.songs(
    song_id,
    title,
    artist_id,
    year,
    duration)
SELECT DISTINCT
    ss.song_id,
    ss.title,
    ss.artist_id,
    ss.year,
    ss.duration
FROM staging.songs as ss
WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO final.artists(
    artist_id,
    name,
    location,
    latitude,
    longitude)
SELECT DISTINCT
    ss.artist_id,
    ss.artist_name,
    ss.artist_location,
    ss.artist_latitude,
    ss.artist_longitude
FROM staging.songs AS ss
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO final.time(
    start_time,
    hour,
    day,
    week,
    weekday,
    month,
    year)
SELECT DISTINCT
    timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
    EXTRACT(hour from start_time) AS hour,
    EXTRACT(day from start_time) AS day,
    EXTRACT(week from start_time) AS week,
    EXTRACT(dow from start_time) AS weekday,
    EXTRACT(month from start_time) AS month,
    EXTRACT(year from start_time) AS year
FROM staging.events as se
WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_schema_queries = [final_schema_create, staging_schema_create]
drop_schema_queries = [final_schema_drop, staging_schema_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
