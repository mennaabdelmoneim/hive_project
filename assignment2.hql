
create database assign2;

!hadoop -mkdir /songs

create external table if not exists songs2
(artist_id string, artist_latitude float , artist_location string, artist_longitude float, artist_name string, duration float , num_songs int, song_id string, title string, year date)
Partitioned by (artist string, year_ string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location 'hdfs://namenode:8020/songs';


!hdfs dfs -put songs.csv /songs;

No, because I didn’t give the partition the location

!hdfs dfs -mkdir -p /songs/artist/year;
alter table songs2
add partition(artist='Marc Shaiman' , year_ ='2003')
location '/songs/artist/year';

!hdfs dfs -put songs.csv /songs/artist/year;


!hadoop fs -ls /songs/artist/year;

create table staging2 (
artist_id string,
artist_latitude string,
artist_location string,
artist_longitude string,
artist_name string,	
duration string,
num_songs string,
song_id	 string,	
title string,	
year string
)
row format delimited
fields terminated by ','
lines terminated by '\n';
load data  local inpath 'songs.csv' into table staging2;


Insert overwrite table songs_extern2 partition (artist_name , year)
select artist_id,
artist_latitude,
artist_location,
artist_longitude,
artist_name,	
duration,
num_songs,
song_id,	
title,	
year 
From staging2 
;

truncate table songs_extern2;
Select * from songs_extern2;


Insert overwrite table songs_extern2 partition (artist_name , year)
select artist_id,
artist_latitude,
artist_location,
artist_longitude,
artist_name,	
duration,
num_songs,
song_id,	
title,	
year 
From staging2 
;


Truncate table songs_EXTERN2;


create table songs_extern2(
    artist_id string,
     artist_latitude string,
     artist_location string,
     artist_longitude string,
     duration string,
     num_songs string,
     song_id string,
     title string
     )
     Partitioned by (year string,artist_name string)
     row format delimited
     fields terminated by ','
     lines terminated by '\n';


Insert overwrite table songs_extern2 partition (year='2007', artist_name)
select artist_id,
artist_latitude,
artist_location,
artist_longitude,
artist_name,	
duration,
num_songs,
song_id,	
title
From staging2 WHERE YEAR='2007'
;


create table new like staging;

avro-tools-1.9.1.jar getschema menna.avro

