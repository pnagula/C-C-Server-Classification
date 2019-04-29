--
-- Query to generate average and stddev functions to each column of network traffic behaviour stats table
--

select 
 'avg('||column_name||') as a'||column_name||','||
 'stddev_pop('||column_name||') as s'||column_name
from information_schema.columns
where table_name ='network_traffic_behaviour_stats'
order by ordinal_position


--
-- Query to generate z-score calculation functions to each column of network traffic behaviour stats table
--

select 
 '('||column_name||'-a'||column_name||')/s'||column_name||' as '||column_name
from information_schema.columns
where table_name ='network_traffic_behaviour_stats'
order by ordinal_position

--
-- query to generate comma separated column list of table network traffic behaviour stats
--
select 
 column_name||','
from information_schema.columns
where table_name ='network_traffic_behaviour_stats'
order by ordinal_position 

--
-- query to generate ANOVA SQL to test variable importance
--

select 
'SELECT '''||column_name||''' as columnname,
       (MADlib.one_way_anova (label,'||column_name||')).*
FROM network_traffic_behaviour_stats_standardized
union all'
 from information_schema.columns
where table_name ='network_traffic_behaviour_stats'
And column_name not in ('ip','label')
order by ordinal_position