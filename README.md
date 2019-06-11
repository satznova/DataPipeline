## **Data Pipeline using Apache Airflow**


### Data Modeling for Sparkify - Song Play Analysis

> Sparkify collects data on songs and user activity on their new music streaming app. The Sparkify analytics team wants to understand what songs users are listening to. The Data modeling for Sparkify makes it easier for their analytics team to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. Due to huge number of users and the data generation getting larger day by day, Cloud (AWS) is used for storing the ingeseted data.

> ETL pipeline is modeled in such a way that the directory of JSON logs and JSON metadata of the songs residing in the Sparkify app is ingested into staging area Amazon S3 and then loaded into Redshift staging tables for transformation. Once the data is in the staging Redshift tables final fact and dimensional tables for start schema are loaded from staging tables. Which in turn be used by the analytics team for querying and understanding what songs the users will be listening to.

> Star Schema data model is used for song play analysis since its very optimal for data analytics because the data in dimensional tables are denormalised and does not need of any join operations on them. Also for the analytics to understand what songs the users will be listening to, aggregation will done. So for large amounts of data Star schema will be optimal.

> Below are the Dimension and Fact tables used for this Star Schema.

##### **Dimension Tables**
1. Users: Users in the app
> (user_id, first_name, last_name, gender, level)
1. Songs: Songs in music database
> (song_id, title, artist_id, year, duration)
1. Artists: Artists in music database
> (artist_id, name, location, lattitude, longitude)
1. Time: timestamps of records in songplays broken down into specific units
> (start_time, hour, day, week, month, year, weekday)

##### **Fact Table**
1. SongPlays: records in log data associated with song plays
> (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)


##### **Source Datasets**

> Two datasets namely, Log data and song data both reside in below S3 buckets:
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data


#### **Execution Steps**

> STEP 1: Create Fact and Dimensional tables in Redshift - execute all create table DDLs present in create_tables.sql script in the Redshift cluster. <br><br>
*It is done manually because table creation must be an one time execution and if the tables get dropped accidentally then we will come to know about during workflow execution.*

> STEP 2: To register Connections and Variables programatically, enter the connection variables in python script init_register_conn.py and run it: <br> `python init_register_conn.py`

> STEP 3: Now, start the DAG from Airflow UI.
