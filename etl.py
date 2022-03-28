import os
import glob
import psycopg2
import pandas as pd
import datetime
from sql_queries import *
from timeit import default_timer as timer


def process_song_file(cur, filepath):
    """
    Read data from json file, ETL the data, and Insert/Load to target table (song, and artist table)
    
    INPUT:
    - cur : psycopg2.connect.cursor. That use to execute PostgreSQL 
    - filepath : string. It's the path of the log file
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = [df.values[0][7], df.values[0][8], df.values[0][0], df.values[0][9], df.values[0][5]]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [df.values[0][0], df.values[0][4], df.values[0][2], df.values[0][1], df.values[0][3]]
    cur.execute(artist_table_insert, artist_data)

def load_from_file(cur, df, sql_copy, sql_insert, sql_truncate, column_key):
    '''
    Try to coding for "Insert data using the COPY command to bulk insert log files instead of using INSERT on one row at a time"
    
    INPUT:
    - cur : psycopg2.connect.cursor. That use to execute PostgreSQL 
    - df : object. The data set that to insert/load to the table.
    - sql_copy : string. The SQL of COPY command of the table.
    - sql_insert : string. The SQL of INSERT command of the table.
    - sql_truncate : string. The SQL of INSERT command of the table.
    - column_key : list. The column name that is the Primary key of the table.
    
    REFERENCE:
    - Idea and Sample for how to write and read data from CSV file ... 
        - [Udacity: copy_from error](https://knowledge.udacity.com/questions/404027)
        - [Github: ciprian90m/data-modelling-with-Postgres/etl.py](https://github.com/ciprian90m/data-modelling-with-Postgres/blob/master/etl.py)
        - [Github: ogierpaul/Udacity_Sparkify_Postgres/sparkify_pg_code/utils.py](https://github.com/ogierpaul/Udacity_Sparkify_Postgres/blob/master/sparkify_pg_code/utils.py)
        - [Github: luke/bulkupsert.py](https://gist.github.com/luke/5697511)
    - Idea for use COPY and UPSERT command ... [Stackoverflow: How Postgresql COPY TO STDIN With CSV do on conflic do update?](https://stackoverflow.com/questions/48019381/how-postgresql-copy-to-stdin-with-csv-do-on-conflic-do-update)
    - Idea and Sample for remove duplicate rows ... [Github: ciprian90m/data-modelling-with-Postgres/etl.py](https://github.com/ciprian90m/data-modelling-with-Postgres/blob/master/etl.py)
    - Idea and Sample for ON CONFLICT DO UPDATE issue ...
        - [Udacity: ON CONFLICT DO UPDATE command cannot affect row a second time](https://knowledge.udacity.com/questions/517736)
        - [Udacity: How to get COPY command to ignore duplicates](https://knowledge.udacity.com/questions/43706)
    '''   
    # Remove Duplicate rows from DataFrame
    df.drop_duplicates(subset = column_key, keep = 'last', inplace = True)
    
    # Prepare csv file for temporary data
    temp_dir = os.path.abspath('.')
    temp_csv = temp_dir + '/temp.csv'
    
    # Save all data in df to csv file
    df.to_csv(temp_csv, encoding='utf-8', sep='|', index=False)
    
    # Open the csv file
    buffer = open(temp_csv, 'r')
    
    # Truncate table before insert new row
    cur.execute(sql_truncate)
    
    # Load all data to the Temp table without any constraint
    cur.copy_expert(sql_copy, buffer)
    
    # Upsert data from Temp table to the Main tables
    cur.execute(sql_insert)
    
    # After use the csv file, it need to remove.
    os.remove(temp_csv)

def process_log_file(cur, filepath):
    """
    Read data from json file, ETL the data, and Insert/Load to target table (time, user, and songplay table)
    
    INPUT:
    - cur : psycopg2.connect.cursor. That use to execute PostgreSQL 
    - filepath : string. It's the path of the log file
    
    REFERENCE:
    - Idea and Sample for using SERIAL datatype from ... [Udacity: songplay_id SERIAL PRIMARY KEY and COPY command](https://knowledge.udacity.com/questions/115607)
    - Idea and Sample for remove duplicate rows ... [Github: ciprian90m/data-modelling-with-Postgres/etl.py](https://github.com/ciprian90m/data-modelling-with-Postgres/blob/master/etl.py)
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    ## Use for loop to insert for each record, I are commented the code for remember original logic.
    #for i, row in time_df.iterrows():
    #    cur.execute(time_table_insert, list(row))
    
    # Use to bulk insert in this function
    load_from_file(cur, time_df, time_table_copy, time_table_insert, time_table_truncate_temp, ['timestamp'])
    
    # load user table
    user_data = (df['userId'], df['firstName'], df['lastName'], df['gender'], df['level'])
    column_labels = ('userId', 'firstName', 'lastName', 'gender', 'level')
    user_df = pd.DataFrame.from_dict(dict(zip(column_labels, user_data)))
    
    # insert user records
    ## Use for loop to insert for each record, I are commented the code for remember original logic.
    #for i, row in user_df.iterrows():
    #    cur.execute(user_table_insert, row)
    
    # Use to bulk insert in this function
    load_from_file(cur, user_df, user_table_copy, user_table_insert, user_table_truncate_temp, ['userId'])

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Get all data files in the path and pass it to right function which is suitable for each data files.
    
    INPUT:
    - cur : psycopg2.connect.cursor. That use to execute PostgreSQL 
    - conn : psycopg2.connect. That use to create connection to PostgreSQL
    - filepath : string. It's the path of the data that indepth from current directory (Not absolute path)
    - func : string. It's the Function name that will process each of data files
    
    REFERENCE:
    - Idea and Sample for use timer in Python ... [Python timeit.default_timer() Examples](https://www.programcreek.com/python/example/12982/timeit.default_timer)
    - Idea and Sample for how to change format of time ... [Stackoverflow: Format timedelta to string](https://stackoverflow.com/questions/538666/format-timedelta-to-string)
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        t_start = timer()
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed, use {}'.format(i, num_files, str(datetime.timedelta(seconds=timer()-t_start))))

def drop_tables_temp(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries_temp` list.
    
    INPUT:
    - cur : psycopg2.connect.cursor. That use to execute PostgreSQL 
    - conn : psycopg2.connect. That use to create connection to PostgreSQL
    """
    
    for query in drop_table_queries_temp:
        cur.execute(query)
        conn.commit()
        
def main():
    """
    Create connection and cursor for access to sparkifydb database. Its Start point for this ETL process. 
    After all insertion successful, it will drop all temp tables and closes the connection.
    
    """
    
    # Create connection and cursor for access the sparkifydb database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    # Start for process and load the song data
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    # Start for process and load the log data
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    print('\n\n***** Summary records each tables *****')
    cur.execute(count_all_tables)
    row = cur.fetchone()
    while row:
       print('The data in "{}" table have {} record(s)'.format(row[0], row[1]))
       row = cur.fetchone()

    # Drop temp tables and close the connection
    drop_tables_temp(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()