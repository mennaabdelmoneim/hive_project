
use assign1_menna;

create table if not exists assign1_menna.assign1_intern_tab
(emp_id int , emp_name string, age int, job_title string, dept_id int, city string, salary int, kilos_from_home int) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

describe formatted  assign1_menna.assign1_intern_tab;

load data local inpath 'employee.csv' into table  assign1_menna.assign1_intern_tab;
!hadoop fs -mkdir /emp_dir;
!hadoop fs -put employee.csv /emp_dir;
load data inpath '/emp_dir/employee.csv' into table  assign1_menna.assign1_intern_tab;

select * from  assign1_menna.assign1_intern_tab limit 10;

create external table if not exists assign1_loc.assign1_ext_tab

(emp_id int , emp_name string, age int, job_title string, dept_id int, city string, salary int, kilos_from_home int)
 ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'hdfs://namenode:8020/emp_dir';


!hadoop fs -put employee.csv /emp_dir;


create table if not exists assign1_intern_tab
(emp_id int , emp_name string, age int, job_title string, dept_id int, city string, salary int, kilos_from_home int) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

create external table if not exists assign1_loc.assign1_ext_tab
(emp_id int , emp_name string, age int, job_title string, dept_id int, city string, salary int, kilos_from_home int)
 ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'hdfs://namenode:8020/emp_dir';


create table if not exists assign1_menna.staging
(emp_id int , emp_name string, age int, job_title string, dept_id int, city string, salary int, kilos_from_home int) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

load data local inpath 'employee.csv' into table assign1_menna.staging;


INSERT INTO assign1_menna.assign1_intern_tab SELECT * FROM assign1_menna.staging; 
INSERT INTO assign1_loc.assign1_ext_tab SELECT * FROM assign1_menna.staging;


describe formatted  assign1_menna.assign1_intern_tab;

describe formatted assign1_loc.assign1_ext_tab;

!wc -l songs.csv;


use assign1_menna;

create table if not exists songs
(artist_id string, artist_latitude float , artist_location string, artist_longitude float, artist_name string, duration float , num_songs int, song_id string, title string, year date)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

load data local inpath 'songs.csv' into table songs;

select * from songs limit 10;

select count(*) from songs;

!hadoop fs -mkdir /songs;


create external table if not exists songs1
(artist_id string, artist_latitude float , artist_location string, artist_longitude float, artist_name string, duration float , num_songs int, song_id string, title string, year date)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'hdfs://namenode:8020/songs';


!hdfs dfs -put songs.csv /songs;

!hadoop fs -cat /songs/songs.csv;

DROP TABLE IF EXISTS assign1_intern_tab;

create table if not exists assign1_intern_tab
(emp_id int , emp_name string, age int, job_title string, dept_id int, city string, salary int, kilos_from_home int) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

load data local inpath 'employee.csv' into table  assign1_intern_tab;




Describe formatted assign1_menna.assign1_intern_tab;

alter table assign1_menna.assign1_intern_tab rename to assign1_loc.assign1_intern_tab;