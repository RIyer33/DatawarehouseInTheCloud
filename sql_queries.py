import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS FactSongplay"
user_table_drop = "DROP TABLE IF EXISTS DimUser"
song_table_drop = "DROP TABLE IF EXISTS DimSong"
artist_table_drop = "DROP TABLE IF EXISTS DimArtist"
time_table_drop = "DROP TABLE IF EXISTS DimTime"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                                        artist varchar,
                                                        auth varchar,
                                                        first_name varchar,
                                                        gender varchar,
                                                        item_in_session int,
                                                        last_name varchar,
                                                        length numeric,
                                                        level varchar,
                                                        location varchar,
                                                        method varchar,
                                                        page varchar,
                                                        registration varchar,
                                                        session_id varchar,
                                                        song varchar,
                                                        status int,
                                                        start_time timestamp,
                                                        user_agent varchar,
                                                        user_id varchar
                                                    )
                                                    DISTSTYLE even
                                                    SORTKEY(start_time);
                
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                                            song_id varchar,
                                                            num_songs int,
                                                            title varchar,
                                                            artist_name varchar,
                                                            latitude numeric,
                                                            year int,
                                                            duration numeric,
                                                            artist_id varchar,
                                                            longitude numeric,
                                                            location varchar
                                                            )
                                                            DISTSTYLE even
                                                            SORTKEY(song_id);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS FactSongplay (
                                                    user_id varchar,
                                                    song_id varchar,
                                                    artist_id varchar,
                                                    session_id varchar,
                                                    start_time timestamp,
                                                    FOREIGN KEY(user_id) REFERENCES DimUser(user_id),
                                                    FOREIGN KEY(song_id) REFERENCES DimSong(song_id),
                                                    FOREIGN KEY(artist_id) REFERENCES DimArtist(artist_id)                                               
                                                    )
                                                    DISTSTYLE even
                                                    SORTKEY(song_id);
""")

user_table_create = (""" cREATE TABLE IF NOT EXISTS DimUser (
                                                    user_id varchar PRIMARY KEY,
                                                    first_name varchar,
                                                    last_name VARCHAR,
                                                    gender varchar,
                                                    level varchar
                                                    )
                                                    DISTSTYLE all
                                                    SORTKEY(user_id);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS DimSong(
                                                    song_id varchar PRIMARY KEY,
                                                    title varchar NOT NULL,
                                                    artist_id varchar NOT NULL,
                                                    year int,
                                                    duration numeric
                                                    )
                                                    DISTSTYLE all
                                                    SORTKEY(song_id);
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS DimArtist(
                                                      artist_id varchar PRIMARY KEY,
                                                      name VARCHAR not null,
                                                      location varchar,
                                                      latitude numeric,
                                                      longitude numeric
                                                      )
                                                      DISTSTYLE all
                                                      SORTKEY(artist_id);
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS DimTime(
                                                    start_time timestamp not null,
                                                    hour int,
                                                    day int,
                                                    week int,
                                                    month int,
                                                    year int,
                                                    weekday int,
                                                    weekday_str varchar(3)
                                                    )
                                                    DISTSTYLE even
                                                    SORTKEY(start_time);
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                            from {} 
                            credentials 'aws_iam_role={}'   
                            json {}  
                            compupdate off
                            region 'us-west-2'
                            timeformat as 'epochmillisecs'
                            truncatecolumns blanksasnull emptyasnull;
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""copy staging_songs 
                                from {} 
                                credentials 'aws_iam_role={}' 
                                JSON 'auto' 
                                truncatecolumns 
                                compupdate off 
                                region \'us-west-2\';
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO FactSongplay (user_id, song_id, artist_id, session_id, start_time)
                                         SELECT 
                                             se.user_id,
                                             sa.song_id,
                                             sa.artist_id,
                                             se.session_id,
                                             se.start_time
                                         FROM staging_events AS se
                                         JOIN (
                                                 SELECT 
                                                     ds.song_id,
                                                     ds.title,
                                                     ds.duration,
                                                     da.artist_id,
                                                     da.name
                                                 FROM DimSong AS ds
                                                 JOIN DimArtist AS da
                                                 ON ds.artist_id = da.artist_id
                                              ) sa
                                         ON (
                                                    se.song = sa.title
                                                AND se.artist = sa.name
                                                AND se.length = sa.duration
                                             )
                                         WHERE se.page = 'NextSong';    
                                         
""")

user_table_insert = (""" INSERT INTO DimUser (user_id, first_name, last_name, gender, level)
                                    (SELECT user_id, first_name, last_name, gender, level
                                     FROM
                                        (
                                            SELECT 
                                                user_id, 
                                                first_name, 
                                                last_name, 
                                                gender, 
                                                level ,
                                                ROW_NUMBER() OVER (PARTITION BY user_id 
                                                                    ORDER BY user_id asc, level desc) AS user_id_rank
                                            FROM staging_events
                                            WHERE page = 'NextSong'
                                            ORDER BY user_id, level
                                            
                                        )
                                       WHERE user_id_rank = 1  
                                    );
""")

song_table_insert = (""" INSERT INTO DimSong (song_id, title, artist_id, year, duration)
                                     (SELECT song_id, title, artist_id, year, duration
                                      FROM
                                          (
                                              SELECT 
                                                  song_id,
                                                  title,
                                                  artist_id,
                                                  year,
                                                  duration,
                                                  ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY song_id) AS song_id_rank
                                               FROM staging_songs
                                               ORDER BY song_id
                                          )
                                        WHERE song_id_rank = 1              
                                     )
""")

artist_table_insert = (""" INSERT INTO DimArtist (artist_id, name, location, latitude, longitude)
                                        (SELECT artist_id, artist_name, location, latitude, longitude
                                         FROM
                                             (SELECT 
                                                 artist_id,
                                                 artist_name,
                                                 location,
                                                 latitude,
                                                 longitude,
                                                 ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_id) AS artist_id_rank
                                              FROM staging_songs   
                                              ORDER BY artist_id
                                              )
                                           WHERE artist_id_rank = 1 
                                        )
""")

time_table_insert = (""" INSERT INTO DimTime (start_time, hour, day, week, month, year, weekday, weekday_str)
                                     (SELECT start_time,
                                             extract(hour FROM start_time) AS hour,
                                             extract(day FROM start_time)  AS day,
                                             extract(week FROM start_time) AS week,
                                             extract(month FROM start_time) AS month,
                                             extract(year FROM start_time) AS year,
                                             extract(dayofweek FROM start_time) AS weekday,
                                             to_char(start_time, 'Dy') AS weekday_str
                                       FROM (
                                               SELECT start_time,
                                                      user_id,
                                                      session_id
                                               FROM (
                                                       SELECT
                                                           start_time,
                                                           user_id,
                                                           session_id,
                                                           page,
                                                           ROW_NUMBER() OVER(PARTITION BY start_time
                                                                               ORDER BY user_id, session_id) AS start_time_rank
                                                       FROM staging_events
                                                       WHERE page = 'NextSong'
                                                       ORDER BY start_time
                                                    )
                                                WHERE start_time_rank = 1
                                            )      
                                     )
""")

#Analytical queries
top_ten_songs = (""" 
                    SELECT TOP 10
                        DRV.title, DRV.num_plays
                    FROM 
                    (
                        SELECT s.title, count(s.title)  as num_plays
                        FROM FactSongplay as fs
                        JOIN DimSong as s
                        ON fs.song_id = s.song_id 
                        JOIN DimArtist as a
                        ON fs.artist_id = a.artist_id 
                        GROUP BY s.title
                    ) AS DRV 
                    ORDER BY DRV.num_plays DESC
                    ;
""")

top_ten_artists = (""" 
                    SELECT 
                        TOP 10 DRV.name, DRV.num_plays
                    FROM 
                    (top_ten_artists
                        SELECT a.name, count(s.title)  as num_plays
                        FROM FactSongplay as fs
                        JOIN DimSong as s
                        ON fs.song_id = s.song_id 
                        JOIN DimArtist as a
                        ON fs.artist_id = a.artist_id 
                        GROUP BY a.name
                    ) AS DRV 
                    ORDER BY DRV.num_plays DESC;
""")

song_play_by_weekday = ("""
                            SELECT dt.weekday_str AS Weekday, count(*) AS Count
                            FROM FactSongplay fs
                            JOIN DimTime      dt ON (dt.start_time  = fs.start_time)
                            GROUP BY dt.weekday, dt.weekday_str
                            ORDER BY dt.weekday;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,  user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
analytical_table_queries = [top_ten_songs, top_ten_artists, song_play_by_weekday]
