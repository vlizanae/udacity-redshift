# Data Warehouse

by Vicente Lizana

## Purpose and Goals of the Database

In the context of the growth of Sparkify, it becomes apparent the need for
a better understanding of the available data, in order to have a better
grasp of what is worth to optimize. For this purpose, a Data Warehouse
seems like the ideal solution.

With AWS as the platform for this purpose, the song data and usage logs are
stored on S3 buckets in JSON format. Later these are staged in Redshift tables
(although RDS could be used for this purpose or the data could come directly
from transactional databases) and finally restructured into a star schema
optimized for analytical queries inside the same Redshift instance.

The goal behind this ETL process is to be able to perform analytical queries,
joining data from different sources efficiently and optimizing resources such
as query times and computational power when trying to answer analytical
questions.

## ETL Pipeline Design

The staging tables are straightforward, the idea is just to replicate the
structure the data already had, but to have the power of a DB to ingest it
into the analytical tables properly.

A **Star Schema** was implemented where 4 dimensions were detected, and a fact
table to store the events. Of the 4 dimensions, the Time dimension was thought
to be the largest one, because there's a single timestamp for every event of
every user.

Because of this, a **Sorting Key** partition strategy was used on this table,
as the timestamps are ordered this will **minimize shuffling** on queries such
as most listened songs or artists over a period of time. To properly minimize
shuffling, the `start_time` was selected as the **Distribution Key** for the
fact table.

To avoid the duplicates from the events staging table, a `SELECT DISTINCT`
clause was used when selecting into the analytical tables as the distributed
nature of Redshift does not allow for conflict detection.

## How to Run the Scripts

To create (or recreate) the database:
```bash
python create_tables.py
```

To run the ETL process (files to staging to warehouse):
```bash
python etl.py
```

## Sample Query

Top 10 months with more usage:
```sql
SELECT
    T.year               AS year,
    T.month              AS month,
    COUNT(P.songplay_id) AS reproductions

FROM songplay P
JOIN time     T ON P.start_time = T.start_time

GROUP BY T.year, T.month
ORDER BY reproductions DESC

LIMIT 10;
```

Top 10 most listened songs on 2018:
```sql
SELECT
    S.title              AS song_title,
    COUNT(P.songplay_id) AS reproductions

FROM songplay P
JOIN song     S ON P.song_id    = S.song_id
JOIN time     T ON P.start_time = T.start_time

WHERE T.year = 2018

GROUP BY S.title
ORDER BY reproductions DESC

LIMIT 10;
```

## Files in this Project

* `sql_queries.py`: To host all of the SQL queries:
    * Drop table queries.
    * Table creation queries (staging and analytical).
    * Copy queries for staging.
    * Insert Into - Select queries for feeding the star schema.
* `create_tables.py`: The logic of running the dropping and creating tables queries.
* `etl.py`: The logic of running the data staging and transformation queries.
* `dwh.cfg` (not in the repo): File with credentials and configurations.
