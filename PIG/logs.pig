#Date 

START2015 = LOAD '/user/vagrant/input/20152014' USING PigStorage(' ') AS (date:chararray, time:chararray, s_ip:chararray,cs_method:chararray, cs_uri_stem:chararray, cs_uri_query:chararray, s_port:int, cs_username:chararray, c_ip:chararray, cs_user_agent:chararray, sc_status:chararray, time_taken:chararray);
DAY2015 = FILTER START2015 BY date != '*#*';
DAY12015= FILTER DAY2015 BY cs_uri_stem != '/' AND cs_uri_stem != '*index.*' AND sc_status == '200';
DAY22015= GROUP DAY12015 BY  (cs_uri_stem,date);
DAY32015= FOREACH DAY22015 GENERATE group , COUNT(DAY12015.cs_uri_stem) as counts;
DAY42015 = ORDER DAY32015 BY counts DESC;
DAY52015 = GROUP DAY42015 BY group.date;
DAY62015 =  foreach DAY52015 generate  MAX(DAY42015.counts) as maxi;
DAY72015 = JOIN DAY42015 BY counts , DAY62015 BY maxi;
DAY82015 = foreach DAY72015 generate $0 , $1;



#Month


START2015 = LOAD '/user/vagrant/input/20152014' USING PigStorage(' ') AS (date:chararray, time:chararray, s_ip:chararray,cs_method:chararray, cs_uri_stem:chararray, cs_uri_query:chararray, s_port:int, cs_username:chararray, c_ip:chararray, cs_user_agent:chararray, sc_status:chararray, time_taken:chararray);
MONTH12015 = FILTER START2015 BY date != '*#*';
MONTH22015= FILTER MONTH12015 BY cs_uri_stem != '/' AND cs_uri_stem != '*index.*' AND sc_status == '200';
MONTH32015 = FOREACH MONTH22015 generate SUBSTRING(date,0,7) as month , cs_uri_stem;
MONTH42015 = GROUP MONTH32015 BY  (cs_uri_stem,month);
MONTH52015 = FOREACH MONTH42015 GENERATE group , COUNT(MONTH32015.cs_uri_stem) as counts;
MONTH62015 = ORDER MONTH52015  BY counts DESC;
MONTH72015 = GROUP MONTH62015 BY group.month;
MONTH82015 =  foreach MONTH72015 generate  MAX(MONTH62015.counts) as maxi;
MONTH92015 = JOIN MONTH62015 BY counts , MONTH82015 BY maxi;
MONTH102015 = foreach MONTH92015 generate $0 , $1;





#Date 

START2016 = LOAD '/user/vagrant/input/20152014' USING PigStorage(' ') AS (date:chararray, time:chararray, s_ip:chararray,cs_method:chararray, cs_uri_stem:chararray, cs_uri_query:chararray, s_port:int, cs_username:chararray, c_ip:chararray, cs_user_agent:chararray, sc_status:chararray, time_taken:chararray);
DAY2016 = FILTER START2016 BY date != '*#*';
DAY12016= FILTER DAY2016 BY cs_uri_stem != '/' AND cs_uri_stem != '*index.*' AND sc_status == '200';
DAY22016= GROUP DAY12016 BY  (cs_uri_stem,date);
DAY32016= FOREACH DAY22016 GENERATE group , COUNT(DAY12016.cs_uri_stem) as counts;
DAY42016 = ORDER DAY32016 BY counts DESC;
DAY52016 = GROUP DAY42016 BY group.date;
DAY62016 =  foreach DAY52016 generate  MAX(DAY42016.counts) as maxi;
DAY72016 = JOIN DAY42016 BY counts , DAY62016 BY maxi;
DAY82016 = foreach DAY72016 generate $0 , $1;



#Month


START2016 = LOAD '/user/vagrant/input/20152014' USING PigStorage(' ') AS (date:chararray, time:chararray, s_ip:chararray,cs_method:chararray, cs_uri_stem:chararray, cs_uri_query:chararray, s_port:int, cs_username:chararray, c_ip:chararray, cs_user_agent:chararray, sc_status:chararray, time_taken:chararray);
MONTH12016 = FILTER START2016 BY date != '*#*';
MONTH22016= FILTER MONTH12016 BY cs_uri_stem != '/' AND cs_uri_stem != '*index.*' AND sc_status == '200';
MONTH32016 = FOREACH MONTH22016 generate SUBSTRING(date,0,7) as month , cs_uri_stem;
MONTH42016 = GROUP MONTH32016 BY  (cs_uri_stem,month);
MONTH52016 = FOREACH MONTH42016 GENERATE group , COUNT(MONTH32016.cs_uri_stem) as counts;
MONTH62016 = ORDER MONTH52016  BY counts DESC;
MONTH72016 = GROUP MONTH62016 BY group.month;
MONTH82016 =  foreach MONTH72016 generate  MAX(MONTH62016.counts) as maxi;
MONTH92016 = JOIN MONTH62016 BY counts , MONTH82016 BY maxi;
MONTH102016 = foreach MONTH92016 generate $0 , $1;
