# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays ;"
user_table_drop = "DROP TABLE IF EXISTS users ;"
song_table_drop = "DROP TABLE IF EXISTS songs ;"
artist_table_drop = "DROP TABLE IF EXISTS artists ;"
time_table_drop = "DROP TABLE IF EXISTS time ;"

# DROP TEMP TABLES

user_table_drop_temp = "DROP TABLE IF EXISTS users_temp ;"
time_table_drop_temp = "DROP TABLE IF EXISTS time_temp ;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays  
        (songplay_id SERIAL PRIMARY KEY, 
        start_time TIMESTAMP NOT NULL REFERENCES time (start_time), 
        user_id INT NOT NULL /*REFERENCES users (user_id)*/, 
        level VARCHAR, 
        song_id VARCHAR, 
        artist_id VARCHAR, 
        session_id INT, 
        location VARCHAR, 
        user_agent VARCHAR) 
;""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
        (user_id INT PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR) 
;""")

user_table_create_BK = ("""CREATE TABLE IF NOT EXISTS users 
        (user_id INT, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR) 
;""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
        (song_id VARCHAR PRIMARY KEY, 
        title VARCHAR, 
        artist_id VARCHAR NOT NULL, 
        year INT, 
        duration NUMERIC) 
;""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
        (artist_id VARCHAR PRIMARY KEY, 
        name VARCHAR, 
        location VARCHAR, 
        latitude NUMERIC, 
        longitude NUMERIC) 
;""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
        (start_time TIMESTAMP PRIMARY KEY, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT) 
;""")

# CREATE TEMP TABLES

user_table_create_temp = ("""CREATE TABLE IF NOT EXISTS users_temp 
        (user_id INT, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR) 
;""")

time_table_create_temp = ("""CREATE TABLE IF NOT EXISTS time_temp 
        (start_time TIMESTAMP, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT) 
;""")

# TRUNCATE TEMP TABLES

user_table_truncate_temp = ("""
    TRUNCATE TABLE users_temp 
;""")

time_table_truncate_temp = ("""
    TRUNCATE TABLE time_temp 
;""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

## Backup code: user_table_insert
user_table_insert_BK = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level) 
    /*SELECT user_id, first_name, last_name, gender, level FROM users_temp */
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO NOTHING
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level) 
    SELECT DISTINCT user_id, first_name, last_name, gender, level FROM users_temp 
    /*VALUES (%s, %s, %s, %s, %s) */
    ON CONFLICT (user_id)
    DO UPDATE 
            SET (first_name, last_name, gender, level) = (EXCLUDED.first_name, EXCLUDED.last_name, EXCLUDED.gender, EXCLUDED.level)
""")

user_table_copy = ("""
    COPY users_temp (user_id, first_name, last_name, gender, level)
    FROM STDIN WITH CSV HEADER ENCODING 'UTF-8' DELIMITER '|'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO UPDATE 
            SET (title, year, duration) = (EXCLUDED.title, EXCLUDED.year, EXCLUDED.duration)
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO UPDATE
            SET (name, location, latitude, longitude) = (EXCLUDED.name, EXCLUDED.location, EXCLUDED.latitude, EXCLUDED.longitude)
""")

## Backup code: time_table_insert
time_table_insert_BK = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
    SELECT start_time, hour, day, week, month, year, weekday FROM time_temp 
    ON CONFLICT (start_time)
    DO NOTHING
""")

time_table_copy = ("""
    COPY time_temp (start_time, hour, day, week, month, year, weekday)
    FROM STDIN WITH CSV HEADER ENCODING 'UTF-8' DELIMITER '|'
""")

# FIND SONGS

song_select = ("""
    SELECT songs.song_id, songs.artist_id 
    FROM songs JOIN artists
    ON songs.artist_id = artists.artist_id 
    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s
;""")

# Summary All Tables

count_all_tables = ("""
    SELECT 'songplays' AS Tablename, COUNT(*) AS CNT FROM songplays
    UNION 
    SELECT 'users' AS Tablename, COUNT(*) AS CNT FROM users
    UNION 
    SELECT 'songs' AS Tablename, COUNT(*) AS CNT FROM songs
    UNION 
    SELECT 'artists' AS Tablename, COUNT(*) AS CNT FROM artists
    UNION 
    SELECT 'time' AS Tablename, COUNT(*) AS CNT FROM time; 
""")


# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]

create_table_queries_temp = [user_table_create_temp, time_table_create_temp]
drop_table_queries_temp = [user_table_drop_temp, time_table_drop_temp]