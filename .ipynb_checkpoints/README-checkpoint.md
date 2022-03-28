# Project: Data Modeling with Postgres

Hi, My name is **Phakphoom Claiboon**. Currently, I in the Data Engineering Nanodegree of Udacity ,and this is my First Project of this course.
Also, it's yet my First Python Project too. It mean Really serious project. I'm more familiar with the SQL language and RDBMS infrastructure like MS SQL Server, DB2 etc.
I know I need to more learning/practice of Python in the future.

Thanks.

## Introduction
A startup called **Sparkify** wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.


## Summary of the Project
The project need to read many json data and ETL them to the data modeling
- Design the Data Model
- Create tables for the Data Model
- ETL process the json data/data files to collect them in the each tables appropriately

## Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
1. songplays - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
2. users - users in the app
    - user_id, first_name, last_name, gender, level
3. songs - songs in music database
    - song_id, title, artist_id, year, duration
4. artists - artists in music database
    - artist_id, name, location, latitude, longitude
5. time - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

## How to run the Python Scripts

1. Run script **create_tables.py** for create/reset the sparkifydb database

``` bash
python create_tables.py
```


2. Run script **etl.py** or **etl.ipynb** for start the ETL process to each tables in the sparkifydb database

``` bash
python etl.py
```


3. Run **test.ipynb** to confirm the creation of your tables with the correct columns. Make sure to click "Restart kernel" to close the connection to the database after running this notebook.

## Example queries and results for song play analysis

Assume that I want to know the 10 songs that hit the charts of Sparkify. Maybe I interesting to listen it someday.
I use the query something like this or you can try it in the **test.ipynb** :

``` sql
SELECT rank() OVER (PARTITION BY songs.title ORDER BY COUNT(*) DESC), songs.title, artists.name , COUNT(*) AS timestoplayed 
FROM songplays 
JOIN songs ON songplays.song_id = songs.song_id 
JOIN artists ON songplays.artist_id = artists.artist_id 
GROUP BY songs.title, artists.name 
LIMIT 10;
```

**Example result as below:**

![Example 10 songs that hit the charts of Sparkify](Example01.png)


> Remark: Since this is a subset of the much larger dataset, the solution dataset will only have 1 row with values for value containing ID for both ```songid``` and ```artistid``` in the fact table. Those are the only 2  values that the query in the ```sql_queries.py``` will return that are not-NONE. The rest of the rows will have NONE values for those two variables.


## Reference

I have been searching for some ideas or other help to find solutions in my code that are shown as below:

1. Idea and Sample for using SERIAL datatype from ... [Udacity: songplay_id SERIAL PRIMARY KEY and COPY command](https://knowledge.udacity.com/questions/115607)
2. Idea and Sample for how to write and read data from CSV file ... 
    - [Udacity: copy_from error](https://knowledge.udacity.com/questions/404027)
    - [Github: ciprian90m/data-modelling-with-Postgres/etl.py](https://github.com/ciprian90m/data-modelling-with-Postgres/blob/master/etl.py)
    - [Github: ogierpaul/Udacity_Sparkify_Postgres/sparkify_pg_code/utils.py](https://github.com/ogierpaul/Udacity_Sparkify_Postgres/blob/master/sparkify_pg_code/utils.py)
    - [Github: luke/bulkupsert.py](https://gist.github.com/luke/5697511)
3. Idea for use COPY and UPSERT command ... [Stackoverflow: How Postgresql COPY TO STDIN With CSV do on conflic do update?](https://stackoverflow.com/questions/48019381/how-postgresql-copy-to-stdin-with-csv-do-on-conflic-do-update)
4. Idea and Sample for use timer in Python ... [Python timeit.default_timer() Examples](https://www.programcreek.com/python/example/12982/timeit.default_timer)
5. Idea and Sample for how to change format of time ... [Stackoverflow: Format timedelta to string](https://stackoverflow.com/questions/538666/format-timedelta-to-string)
6. Idea and Sample for remove duplicate rows ... [Github: ciprian90m/data-modelling-with-Postgres/etl.py](https://github.com/ciprian90m/data-modelling-with-Postgres/blob/master/etl.py)
7. Idea and Sample for ON CONFLICT DO UPDATE issue ...
    - [Udacity: ON CONFLICT DO UPDATE command cannot affect row a second time](https://knowledge.udacity.com/questions/517736)
    - [Udacity: How to get COPY command to ignore duplicates](https://knowledge.udacity.com/questions/43706)