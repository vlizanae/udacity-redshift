import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

config_s3 = dict(config['S3'].items())
config_iam_role = dict(config['IAM_ROLE'].items())

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay;"
user_table_drop           = "DROP TABLE IF EXISTS sparkify_user;"
song_table_drop           = "DROP TABLE IF EXISTS song;"
artist_table_drop         = "DROP TABLE IF EXISTS artist;"
time_table_drop           = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist         VARCHAR,
    auth           VARCHAR,
    firstName      VARCHAR,
    gender         CHAR,
    itemInSession  INTEGER,
    lastName       VARCHAR,
    length         FLOAT,
    level          VARCHAR,
    location       VARCHAR,
    method         VARCHAR,
    page           VARCHAR,
    registration   BIGINT,
    sessionId      INTEGER,
    song           VARCHAR,
    status         INTEGER,
    ts             TIMESTAMP,
    userAgent      VARCHAR,
    userId         INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs         INTEGER,
    artist_id         VARCHAR,
    artist_latitude   FLOAT,
    artist_longitude  FLOAT,
    artist_location   VARCHAR,
    artist_name       VARCHAR,
    song_id           VARCHAR,
    title             VARCHAR,
    duration          FLOAT,
    year              SMALLINT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id  INTEGER    IDENTITY PRIMARY KEY,
    start_time   TIMESTAMP  NOT NULL REFERENCES time DISTKEY,
    user_id      INTEGER    NOT NULL REFERENCES sparkify_user,
    song_id      VARCHAR    REFERENCES song,
    artist_id    VARCHAR    REFERENCES artist,
    session_id   INTEGER,
    location     VARCHAR,
    user_agent   VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS sparkify_user (
    user_id     INTEGER  PRIMARY KEY,
    first_name  VARCHAR,
    last_name   VARCHAR,
    gender      CHAR,
    level       VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
    song_id    VARCHAR   PRIMARY KEY,
    title      VARCHAR,
    artist_id  VARCHAR,
    year       SMALLINT,
    duration   FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
    artist_id  VARCHAR  PRIMARY KEY,
    name       VARCHAR,
    location   VARCHAR,
    latitude   FLOAT,
    longitude  FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP  PRIMARY KEY SORTKEY DISTKEY,
    hour        SMALLINT,
    day         SMALLINT,
    week        SMALLINT,
    month       SMALLINT,
    year        SMALLINT,
    weekday     SMALLINT
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY
    staging_events
FROM
    {log_data}
JSON
    {log_jsonpath}
TIMEFORMAT
    'epochmillisecs'
IAM_ROLE
    {arn}
REGION
    'us-west-2';
""").format(**config_s3, **config_iam_role)

staging_songs_copy = ("""
COPY
    staging_songs
FROM
    {song_data}
JSON
    'auto'
IAM_ROLE
    {arn}
REGION
    'us-west-2';
""").format(**config_s3, **config_iam_role)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO
    songplay (start_time, user_id, song_id, artist_id, session_id, location, user_agent)

SELECT
    ts         AS start_time,
    userId     AS user_id,
    song_id,
    artist_id,
    sessionId  AS session_id,
    location,
    userAgent  AS user_agent
    
FROM      staging_events E
LEFT JOIN staging_songs  S ON E.song=S.title

WHERE
    page='NextSong';
""")

user_table_insert = ("""
INSERT INTO
    sparkify_user

SELECT DISTINCT
    userId     AS user_id,
    firstName  As first_name,
    lastName   As last_name,
    gender,
    level
    
FROM staging_events

WHERE
    page='NextSong';
""")

song_table_insert = ("""
INSERT INTO
    song
    
SELECT
    song_id,
    title,
    artist_id,
    year,
    duration

FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO
    artist
    
SELECT DISTINCT
    artist_id,
    artist_name       AS name,
    artist_location   AS location,
    artist_latitude   AS latitude,
    artist_longitude  AS longitude
    
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO
    time
    
SELECT DISTINCT
    ts                        AS start_time,
    EXTRACT(hour FROM ts)     AS hour,
    EXTRACT(day FROM ts)      AS day,
    EXTRACT(week FROM ts)     AS week,
    EXTRACT(month FROM ts)    AS month,
    EXTRACT(year FROM ts)     AS year,
    EXTRACT(weekday FROM ts)  AS weekday
    
FROM staging_events

WHERE
    page='NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
