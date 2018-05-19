Item-04 - Hive 
---------------

Hive is a great tool to work on data . It is a framework for datawarehousing . 

In this assignment we initially create tables and then load the data of the weblogs. Hive reacts much faster when compared to Sqoop ormySQL. 

Below are the commands for creating the table and inserting the records 

**Table creation **

create table weblogsmall( dates varchar(50), time varchar(50), s_ip varchar(50),cs_method varchar(50), cs_uri_stem varchar(50), 
cs_uri_query varchar(50), s_port int, cs_username varchar(50), c_ip varchar(50), cs_user_agent varchar(50), sc_status varchar(50), 
sc_substatus varchar(50), sc_win32_status varchar(50), time_taken varchar(50) )
ROW FORMAT DELIMITED  FIELDS TERMINATED BY ' '; 



create table weblog( dates varchar(50), time varchar(50), s_ip varchar(50),cs_method varchar(50), cs_uri_stem varchar(50), 
cs_uri_query varchar(50), s_port int, cs_username varchar(50), c_ip varchar(50), cs_user_agent varchar(50), sc_status varchar(50), 
sc_substatus varchar(50), sc_win32_status varchar(50), time_taken varchar(50) )
ROW FORMAT DELIMITED  FIELDS TERMINATED BY ' '; 

**Record Insertion**

load data local INPATH '/vagrant/weblogs/20152014/20152014' overwrite into table weblog;

load data local INPATH '/vagrant/weblogs/2016/2016' overwrite into table weblog;

Queries :
---------

**weblogs**

Date

select dates, cs_uri_stem , max(counts) from ( select dates, cs_uri_stem , count(cs_uri_stem) as counts from weblogsmall where 
cs_uri_stem NOT LIKE '/' and cs_uri_stem NOT LIKE '%index%' and sc_status like '200' group by cs_uri_stem , dates ) as webs 
group by dates , distinct(cs_uri_stem) order by dates;

Month

select month,cs_uri_stem,max(counts) from ( select SUBSTRING(dates,1,7) , cs_uri_stem , count(cs_uri_stem) as counts , count(cs_uri_stem) from weblogsmall where 
cs_uri_stem NOT LIKE '/' and cs_uri_stem NOT LIKE '%index%' and sc_status like '200' group by cs_uri_stem , SUBSTRING(dates,1,7)) as websmonth
group by month order by month;


**weblog** 

Date

select dates, cs_uri_stem , max(counts) from ( select dates, cs_uri_stem , count(cs_uri_stem) as counts from weblog where 
cs_uri_stem NOT LIKE '/' and cs_uri_stem NOT LIKE '%index%' and sc_status like '200' group by cs_uri_stem , dates ) as webs 
group by dates , distinct(cs_uri_stem) order by dates;

Month

select month,cs_uri_stem,max(counts) from ( select SUBSTRING(dates,1,7) , cs_uri_stem , count(cs_uri_stem) as counts , count(cs_uri_stem) from weblog where 
cs_uri_stem NOT LIKE '/' and cs_uri_stem NOT LIKE '%index%' and sc_status like '200' group by cs_uri_stem , SUBSTRING(dates,1,7)) as websmonth
group by month order by month;

Screenshots:
------------

![1](https://cloud.githubusercontent.com/assets/17997235/25561527/7bbcd174-2d34-11e7-991b-88f29108cae0.JPG)

![2](https://cloud.githubusercontent.com/assets/17997235/25561526/7bbc8610-2d34-11e7-9202-0c6626425df1.JPG)

![3](https://cloud.githubusercontent.com/assets/17997235/25561529/7bbdef46-2d34-11e7-9d9a-8d89aa493d72.JPG)

![4](https://cloud.githubusercontent.com/assets/17997235/25561530/7bbff8ae-2d34-11e7-84b9-f860f0c39e84.JPG)

![5](https://cloud.githubusercontent.com/assets/17997235/25561528/7bbdc5f2-2d34-11e7-9e1c-c84fe47f9a1e.JPG)

![6](https://cloud.githubusercontent.com/assets/17997235/25561531/7bc49a58-2d34-11e7-88b0-b28f871907dc.JPG)

![7](https://cloud.githubusercontent.com/assets/17997235/25561532/7bc5380a-2d34-11e7-962f-3bf63573d034.JPG)

