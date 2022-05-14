
create table if not exists event_tab(
artist string,
auth string,
firstName string,
gender string,
itemInSession string,
lastName string,
length string,
level string,
location string,
method string,
page string,
registration string,
sessionId string,
song string,
status string,
ts string,
userAgent string,
userId string
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

load data local inpath 'events.csv' into table event_tab;


Select userId, song, sessionId, last_value(song)over(partition by sessionId order by itemInSession ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following), first_value(song)over(partition by sessionId order by itemInSession ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) 
 from event_tab limit 50;

SELECT userId,count(distinct song), RANK() OVER  (Order BY COUNT(distinct song) DESC) FROM event_tab group by userId;



SELECT userId,count(distinct song), Row_number() OVER  (Order BY COUNT(distinct song) DESC) FROM event_tab group by userId;


SELECT COUNT(song) FROM event_tab GROUP BY location, artist
GROUPING SETS ((location,artist),location,());


SELECT COUNT(song) FROM event_tab GROUP BY location, artist
GROUPING SETS ((location,artist),location, artist, ());


select userId,song ,ts from event_tab 
order by userId,song, ts;



select userId,song ,ts from event_tab 
cluster by userId, song, ts;

