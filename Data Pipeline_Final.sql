
--
-- function to convert hex format TCP flags to 8-bit format chars
--

drop function if exists hex_to_bits(varchar);
CREATE OR REPLACE FUNCTION hex_to_bits(hexval varchar) RETURNS char(8) AS $$
DECLARE
   result  char(8);
BEGIN
 EXECUTE 'SELECT ((x''' || hexval || '''::int)::bit(8))::char(8)' INTO result;  RETURN result;
END; 
$$ LANGUAGE 'plpgsql' IMMUTABLE STRICT;

--
-- enrich bigint format timestamps to real timestamps and calculate duration of flow
--

drop table if exists ipfix_stage1;
create table public.ipfix_stage1 as 
SELECT
       "timestamp" as load_ts
       , TO_TIMESTAMP("timestamp" )::timestamp without time zone as rec_timestamp
       , flowstartsec as fstart_ts
       , TO_TIMESTAMP(flowstartsec)::timestamp without time zone as flow_start_ts
       , flowendsec as fend_ts
       , TO_TIMESTAMP(flowendsec)::timestamp without time zone as flow_end_ts
       , EXTRACT(EPOCH FROM (TO_TIMESTAMP(flowendsec)::timestamp without time zone -TO_TIMESTAMP(flowstartsec)::timestamp without time zone)) as duration_secs
       , dip as dst_ip
       , dport as dst_port
       , protocol
       , sip as src_ip
       , sport as src_port
       , packetcount as packets
       , inbytes as bytes
       , tcpflag as tcp_flags
       , hex_to_bits(tcpflag) as binary_tcp_flags
from public.ipfix_netflow_raw
distributed randomly;


--
-- Select unique recrods from ipfix_stage1
--


drop table if exists ipfix_stage2;
create table ipfix_stage2 as 
select distinct src_ip,dst_ip,src_port,dst_port,duration_secs,protocol,flow_start_ts,flow_end_ts,bytes,packets,tcp_flags,binary_tcp_flags 
from ipfix_stage1
where duration_secs >=0 and src_ip is not null and dst_ip is not null
and  src_ip not in (select ip from ip_omit_list) and dst_ip not in (select ip from ip_omit_list)
and  src_ip not like '8.8.8.%' and dst_ip not like '8.8.8.%' and  src_ip not like '8.8.4.%' and  dst_ip not like '8.8.4.%' 
and  src_ip not like '74.125.%' and dst_ip not like '74.125.%' and  src_ip not like '172.217.%' and dst_ip not like '172.217.%'
and  extract(year from flow_start_ts) = 2018
Distributed randomly ;


--
-- Calculate time interval between current row timestamp and previous row timestamp (rows are ordered in flow_start_ts), enrich TCP flags into separate dummy variables
--
-- uncomment the below drop and create only when you want to do timeslot calculations
--drop table if exists ipfix_stage3;
--create table ipfix_stage3 as 
drop table if exist ipfix_enriched;
create table ipfix_enriched as 
select 
	flow_start_ts,
	flow_end_ts,
	case when duration_secs = 0 then bytes else bytes/duration_secs*1.0 end as bytespersec,
	duration_secs,
	dst_ip,
	dst_port,
	protocol,
	src_ip,
	src_port,
	packets,
	bytes,
	tcp_flags,
	binary_tcp_flags,
	case when substring(binary_tcp_flags,1,1) = '1' then
	     1
	     else
	     0
	end as cwr_flag,
	case when substring(binary_tcp_flags,2,1) = '1' then
	     1
	     else
	     0
	end as ece_flag,
	case when substring(binary_tcp_flags,3,1) = '1' then
	     1
	     else
	     0
	end as urg_flag,
	case when substring(binary_tcp_flags,4,1) = '1' then
	     1
	     else
	     0
	end as ack_flag,
	case when substring(binary_tcp_flags,5,1) = '1' then
	     1
	     else
	     0
	end as push_flag,
	case when substring(binary_tcp_flags,6,1) = '1' then
	     1
	     else
	     0
	end as res_flag,
	case when substring(binary_tcp_flags,7,1) = '1' then
	     1
	     else
	     0
	end as syn_flag,
	case when substring(binary_tcp_flags,8,1) = '1' then
	     1
	     else
	     0
	end as finish_flag
	--uncomment below line of code only when you want to do timeslot calculations
	--,flow_start_ts - first_value(flow_start_ts) OVER  (order by flow_start_ts) running_total_mins
from ipfix_stage2 
Distributed randomly
;

-- uncomment below two SQL only when you want to do timeslot calculations
--
-- convert interval into seconds
--

--drop table if exists ipfix_stage4;
--create table ipfix_stage4 as
--select *,
--      extract(day from running_total_mins)*24*3600+extract(hour from running_total_mins)*3600 + extract(minute from running_total_mins)*60 + extract(second from running_total_mins) as total_secs
--From ipfix_stage3
--Distributed randomly;

--
-- Index row into one of the Timeslot 5 Minutes - 1 day 
--

--drop table if exists ipfix_enriched;
--create table ipfix_enriched as 
--select *,
--      floor(total_secs/300) as timeslot_5m,
--       floor(total_secs/600) as timeslot_10m,
--       floor(total_secs/900) as timeslot_15m,
--       floor(total_secs/1800) as timeslot_30m,
--       floor(total_secs/3600) as timeslot_1h,
--       floor(total_secs/7200) as timeslot_2h,
--       floor(total_secs/14400) as timeslot_4h,
--       floor(total_secs/86400) as timeslot_1d 
--from ipfix_stage4
--distributed randomly
--;


--
-- Aggregate on Source ips , compute mean,standard deviation, iqr, min, max, range, median of "bytes, packets, bytespersec, duration of flow"
--

drop table if exists network_traffic_aggr_stage1_srcip;
create table network_traffic_aggr_stage1_srcip as 
select         		 src_ip as ip ,

			 -- average and standard deviation of  packets,bytes,duration secs, bytes per second
			 
			 avg(packets)         as avgpkts_sent ,   stddev_pop(packets)        as stdpkts_sent ,
			 avg(bytes)           as avgbytes_sent ,  stddev_pop(bytes)          as stdbytes_sent , 
			 avg(duration_secs)   as avgdur_sent ,    stddev_pop(duration_secs)  as stddur_sent ,
			 avg(bytespersec)     as avgbps_sent ,    stddev_pop(bytespersec)  as stdbps_sent ,

			 -- count of distinct source and destination ports, number of flows
			 
			 count(distinct dst_port)  as count_distinct_dst_ports_sent, count(distinct src_port)  as count_distinct_source_ports_sent, count(*) as count_of_flows_sent,
			 
			 -- iqr of packets,bytes,duration secs, bytes per second
			 
			 percentile_cont(.75) within group (order by bytes) - percentile_cont(.25) within group (order by bytes)  as iqrbytes_sent,
			 percentile_cont(.75) within group (order by packets) - percentile_cont(.25) within group (order by packets)  as iqrpkts_sent,
			 percentile_cont(.75) within group (order by duration_secs) - percentile_cont(.25) within group (order by duration_secs)  as iqrdur_sent,
			 percentile_cont(.75) within group (order by bytespersec) - percentile_cont(.25) within group (order by bytespersec)  as iqrbps_sent,

			 -- min and max of packets,bytes,duration secs, bytes per second
			
			 min(packets) as minpkts_sent,max(packets) as maxpkts_sent,
			 min(bytes) as minbytes_sent, max(bytes) as maxbytes_sent,
			 min(duration_secs) as mindur_sent, max(duration_secs) as maxdur_sent,
			 min(bytespersec) as minbps_sent,  max(bytespersec) as maxbps_sent, 

			 -- range of packets,bytes,duration secs, bytes per second

			 max(packets)   - min(packets)  as rangepkts_sent,
			 max(bytes)  - min(bytes)  as rangebytes_sent,
			 max(duration_secs)   - min(duration_secs) as rangedur_sent,
			 max(bytespersec) - min(bytespersec) as rangebps_sent,

			 -- median of packets,bytes,duration secs, bytes per second
			 
			 median(bytes) as medbytes_sent,
			 median(packets) as medpkts_sent,
			 median(duration_secs) as meddur_sent,
			 median(bytespersec) as medbps_sent,

			 -- out degree
			 count(distinct dst_ip) + 1 as out_degree
			 
from ipfix_enriched
group by src_ip
distributed by (ip);


--
-- Aggregate on Destination ips , compute mean,standard deviation, iqr, min, max, range, median of "bytes, packets, bytespersec, duration of flow"
--

drop table if exists network_traffic_aggr_stage1_dstip;
create table network_traffic_aggr_stage1_dstip as 
select          dst_ip as ip ,

			 -- average and standard deviation of  packets,bytes,duration secs, bytes per second
			 
			 avg(packets)         as avgpkts_recev ,   stddev_pop(packets)        as stdpkts_recev ,
			 avg(bytes)           as avgbytes_recev ,  stddev_pop(bytes)          as stdbytes_recev , 
			 avg(duration_secs)   as avgdur_recev ,    stddev_pop(duration_secs)  as stddur_recev ,
			 avg(bytespersec)     as avgbps_recev ,    stddev_pop(bytespersec)  as stdbps_recev ,

			 -- count of distinct source and destination ports, number of flows
			 
			 count(distinct dst_port)  as count_distinct_dst_ports_recev, count(distinct src_port)  as count_distinct_source_ports_recev, count(*) as count_of_flows_recev,
			 
			 -- iqr of packets,bytes,duration secs, bytes per second
			 
			 percentile_cont(.75) within group (order by bytes) - percentile_cont(.25) within group (order by bytes)  as iqrbytes_recev,
			 percentile_cont(.75) within group (order by packets) - percentile_cont(.25) within group (order by packets)  as iqrpkts_recev,
			 percentile_cont(.75) within group (order by duration_secs) - percentile_cont(.25) within group (order by duration_secs)  as iqrdur_recev,
			 percentile_cont(.75) within group (order by bytespersec) - percentile_cont(.25) within group (order by bytespersec)  as iqrbps_recev,

			 -- min and max of packets,bytes,duration secs, bytes per second
			
			 min(packets) as minpkts_recev,max(packets) as maxpkts_recev,
			 min(bytes) as minbytes_recev, max(bytes) as maxbytes_recev,
			 min(duration_secs) as mindur_recev, max(duration_secs) as maxdur_recev,
			 min(bytespersec) as minbps_recev,  max(bytespersec) as maxbps_recev, 

			 -- range of packets,bytes,duration secs, bytes per second

			 max(packets)   - min(packets)  as rangepkts_recev,
			 max(bytes)  - min(bytes)  as rangebytes_recev,
			 max(duration_secs)   - min(duration_secs) as rangedur_recev,
			 max(bytespersec) - min(bytespersec) as rangebps_recev,

			 -- median of packets,bytes,duration secs, bytes per second
			 
			 median(bytes) as medbytes_recev,
			 median(packets) as medpkts_recev,
			 median(duration_secs) as meddur_recev,
			 median(bytespersec) as medbps_recev,

			 -- in degree
			count(distinct src_ip) + 1 as in_degree
			 
from ipfix_enriched
group by dst_ip
distributed by (ip);

--
-- combine source and destination stats as Sent and received for each IP.
--

	
drop table if exists network_traffic_aggr;
create table network_traffic_aggr as 
select 
	case when b.ip is null then 
		a.ip
	     when a.ip is null then
		b.ip
	    else
		b.ip
	end as ip,   
        coalesce(avgpkts_sent,0) as avgpkts_sent, coalesce(stdpkts_sent,0) as stdpkts_sent,
        coalesce(avgbytes_sent,0) as avgbytes_sent, coalesce(stdbytes_sent,0) as stdbytes_sent,
        coalesce(avgdur_sent,0) as avgdur_sent, coalesce(stddur_sent,0) as stddur_sent,
        coalesce(avgbps_sent,0) as  avgbps_sent , coalesce(stdbps_sent,0) as stdbps_sent,
        coalesce(count_distinct_dst_ports_sent,0) as count_distinct_dst_ports_sent, coalesce(count_distinct_source_ports_sent,0) as count_distinct_source_ports_sent, coalesce(count_of_flows_sent,0) as count_of_flows_sent,
        coalesce(iqrbytes_sent,0) as iqrbytes_sent, coalesce(iqrpkts_sent,0) as iqrpkts_sent, coalesce(iqrdur_sent,0) as iqrdur_sent, coalesce(iqrbps_sent,0) as iqrbps_sent,
        coalesce(minpkts_sent,0) as minpkts_sent, coalesce(maxpkts_sent,0) as maxpkts_sent,
        coalesce(minbytes_sent,0) as minbytes_sent, coalesce(maxbytes_sent,0) as maxbytes_sent,
        coalesce(mindur_sent,0) as mindur_sent, coalesce(maxdur_sent,0) as maxdur_sent,
        coalesce(minbps_sent,0) as minbps_sent, coalesce(maxbps_sent,0) as maxbps_sent,
	coalesce(rangepkts_sent,0) as rangepkts_sent, coalesce(rangebytes_sent,0) as rangebytes_sent, coalesce(rangedur_sent,0) as rangedur_sent, coalesce(rangebps_sent,0) as rangebps_sent,
	coalesce(medbytes_sent,0) as medbytes_sent, coalesce(medpkts_sent,0) as medpkts_sent, coalesce(meddur_sent,0) as meddur_sent, coalesce(medbps_sent,0) as medbps_sent,

	coalesce(avgpkts_recev,0) as avgpkts_recev, coalesce(stdpkts_recev,0) as stdpkts_recev,
        coalesce(avgbytes_recev,0) as avgbytes_recev, coalesce(stdbytes_recev,0) as stdbytes_recev,
        coalesce(avgdur_recev,0) as avgdur_recev, coalesce(stddur_recev,0) as stddur_recev,
        coalesce(avgbps_recev,0) as  avgbps_recev , coalesce(stdbps_recev,0) as stdbps_recev,
        coalesce(count_distinct_dst_ports_recev,0) as count_distinct_dst_ports_recev, coalesce(count_distinct_source_ports_recev,0) as count_distinct_source_ports_recev, coalesce(count_of_flows_recev,0) as count_of_flows_recev,
        coalesce(iqrbytes_recev,0) as iqrbytes_recev, coalesce(iqrpkts_recev,0) as iqrpkts_recev, coalesce(iqrdur_recev,0) as iqrdur_recev, coalesce(iqrbps_recev,0) as iqrbps_recev,
        coalesce(minpkts_recev,0) as minpkts_recev, coalesce(maxpkts_recev,0) as maxpkts_recev,
        coalesce(minbytes_recev,0) as minbytes_recev, coalesce(maxbytes_recev,0) as maxbytes_recev,
        coalesce(mindur_recev,0) as mindur_recev, coalesce(maxdur_recev,0) as maxdur_recev,
        coalesce(minbps_recev,0) as minbps_recev, coalesce(maxbps_recev,0) as maxbps_recev,
	coalesce(rangepkts_recev,0) as rangepkts_recev, coalesce(rangebytes_recev,0) as rangebytes_recev, coalesce(rangedur_recev,0) as rangedur_recev, coalesce(rangebps_recev,0) as rangebps_recev,
	coalesce(medbytes_recev,0) as medbytes_recev, coalesce(medpkts_recev,0) as medpkts_recev, coalesce(meddur_recev,0) as meddur_recev, coalesce(medbps_recev,0) as medbps_recev,
	case when out_degree is null then 1/(in_degree*1.0)
 	     when in_degree is null then out_degree*1.0
	     else out_degree/(in_degree*1.0)
        End as in_out_ratio,coalesce(in_degree,0) as in_degree ,coalesce(out_degree,0)	as out_degree,
        coalesce(count_distinct_dst_ports_sent,0)/(coalesce(count_of_flows_sent,0)*1.0+1) as pct_dst_ports_flows_per_ip_sent,
        coalesce(count_distinct_source_ports_sent,0)/(coalesce(count_of_flows_sent,0)*1.0+1) as pct_source_ports_flows_per_ip_sent,
        coalesce(count_distinct_dst_ports_recev,0)/(coalesce(count_of_flows_recev,0)*1.0+1) as pct_dst_ports_flows_per_ip_recev,
        coalesce(count_distinct_source_ports_recev,0)/(coalesce(count_of_flows_recev,0)*1.0+1) as pct_source_ports_flows_per_ip_recev

from  network_traffic_aggr_stage1_srcip a full outer join network_traffic_aggr_stage1_dstip b
on a.ip=b.ip
distributed by (ip);


--
-- for Source ips, compute absolute deviation for bytes, packets, bytespersec and duration of flow from its own median
--

drop table if exists mad_stats_srcip_1;
create table mad_stats_srcip_1 as 
select 
	a.src_ip as ip,abs(bytes - medbytes_sent) as devbytes_sent,abs(packets - medpkts_sent) as devpkts_sent,abs(duration_secs - meddur_sent) as devdur_sent,abs(bytespersec - medbps_sent) as devbps_sent
from ipfix_enriched a , network_traffic_aggr b
Where a.src_ip=b.ip
Distributed by (ip);

--
-- Aggregate on Source ips, compute median of absolute deviation (MAD) for bytes, packets, bytespersec and duration of flow
--

drop table if exists mad_stats_srcip;
create table mad_stats_srcip as 
select 
	ip, median(devbytes_sent) as madbytes_sent,median(devpkts_sent) as madpkts_sent,median(devdur_sent) as maddur_sent,median(devbps_sent) as madbps_sent
from mad_stats_srcip_1
group by ip
distributed by (ip);

--
-- for destination ips, compute absolute deviation for bytes, packets, bytespersec and duration of flow from its own median
--

drop table if exists mad_stats_dstip_1;
create table mad_stats_dstip_1 as 
select 
	a.dst_ip as ip,abs(bytes - medbytes_recev) as devbytes_recev,abs(packets - medpkts_recev) as devpkts_recev,abs(duration_secs - meddur_recev) as devdur_recev,abs(bytespersec - medbps_recev) as devbps_recev
from ipfix_enriched a ,  network_traffic_aggr b
Where a.dst_ip=b.ip
Distributed by (ip);

--
-- Aggregate on Destination ips, compute median absolute deviation for bytes, packets, bytespersec and duration of flow
--

drop table if exists mad_stats_dstip;
create table mad_stats_dstip as 
select 
	ip, median(devbytes_recev) as madbytes_recev,median(devpkts_recev) as madpkts_recev,median(devdur_recev) as maddur_recev,median(devbps_recev) as madbps_recev
from mad_stats_dstip_1
group by ip
distributed by (ip);

--
-- combine source and destination MAD stats for each IP 
--

drop table if exists network_traffic_mad_stats;
create table network_traffic_mad_stats as
select case when e.ip is null then 
		d.ip
	     when d.ip is null then
		e.ip
	    else
		d.ip
	end as ip,  
	coalesce(madbytes_sent,0) as madbytes_sent,coalesce(madbytes_recev,0) as madbytes_recev,
	coalesce(madpkts_sent,0) as madpkts_sent,coalesce(madpkts_recev,0) as madpkts_recev,
	coalesce(maddur_sent,0) as maddur_sent,coalesce(maddur_recev,0) as maddur_recev,
	coalesce(madbps_sent,0) as madbps_sent,coalesce(madbps_recev,0) as madbps_recev
from 
	mad_stats_srcip d full outer join mad_stats_dstip e
on d.ip=e.ip
distributed by (ip)
;


--
-- count/frequency of specific bytes by source IP
--

drop table if exists frequency_stats_bytes_sent_1;
create table frequency_stats_bytes_sent_1 as 
select	 src_ip as ip
	,bytes as bytes_sent
	,count(*) as bytcnt_sent 						-- count number of times specific number of bytes sent by IP
from ipfix_enriched
group by src_ip,bytes
distributed by (ip)
;

--
-- count/frequency of specific bytes by destination IP
--

drop table if exists frequency_stats_bytes_recev_1;
create table frequency_stats_bytes_recev_1 as 
select	 dst_ip as ip
	,bytes as bytes_recev
	,count(*) as bytcnt_recev 						-- count number of times specific number of bytes received by IP
from ipfix_enriched
group by dst_ip,bytes
distributed by (ip)
;	

--
-- rank distinct bytes by their number of times sent by IP 
--

Drop table if exists frequency_stats_bytes_sent_2;
Create table frequency_stats_bytes_sent_2 as 
select 
	    ip,
    	    bytes_sent, 
	    bytcnt_sent,
	    ROW_NUMBER() OVER(PARTITION BY ip ORDER BY bytcnt_sent DESC,bytes_sent) AS  bytrank_sent			-- rank distinct bytes by their number of times sent by ips 
from frequency_stats_bytes_sent_1
Distributed by (ip);

--
-- rank distinct bytes by their number of times received by IP
--

drop table if exists frequency_stats_bytes_recev_2;
create table frequency_stats_bytes_recev_2 as 
select 	      ip,
      	      bytes_recev, 
              bytcnt_recev,
              ROW_NUMBER() OVER(PARTITION BY ip ORDER BY bytcnt_recev DESC,bytes_recev) AS  bytrank_recev			-- rank distinct bytes by their number of times received by ips 
from frequency_stats_bytes_recev_1
Distributed by (ip);

--
-- Select Most Frequent bytes sent by IP
--

Drop table if exists frequency_stats_bytes_sent;
Create table frequency_stats_bytes_sent as 
Select *
from frequency_stats_bytes_sent_2
where bytrank_sent = 1
Distributed by (ip);

--
-- Select Most Frequent bytes received by IP
--

Drop table if exists frequency_stats_bytes_recev;
Create table frequency_stats_bytes_recev as 
Select *
from frequency_stats_bytes_recev_2
Where bytrank_recev = 1
Distributed by (ip);

--
-- Combine source and destination to get Most frequent byte sent and received by IP
--
			
drop table if exists frequency_stats_bytes;
create table frequency_stats_bytes as 
select 
	case when c.ip is null then 
		d.ip
	     when d.ip is null then
		c.ip
	    else
		c.ip
	end as ip,
	coalesce(bytes_sent,0) as frequent_bytes_sent,coalesce(bytes_recev,0) as frequent_bytes_recev,
	coalesce(bytcnt_sent,0) as frequency_of_frequent_bytes_sent,coalesce(bytcnt_recev,0) as frequency_of_frequent_bytes_recev
from	frequency_stats_bytes_sent c full outer join frequency_stats_bytes_recev d
on c.ip=d.ip
distributed by (ip)
;


--
-- count/frequency of specific packets sent by IP  
--


drop table if exists frequency_stats_pkts_sent_1;
create table frequency_stats_pkts_sent_1 as 
select	 src_ip as ip
	,packets as packets_sent
	,count(*) as pktcnt_sent 						-- count number of times specific number of bytes occured between source and destination ips
from ipfix_enriched
group by src_ip,packets
distributed by (ip)
;

--
-- count/frequency of specific packets received by IP
--

drop table if exists frequency_stats_pkts_recev_1;
create table frequency_stats_pkts_recev_1 as 
select	 dst_ip as ip
	,packets as packets_recev
	,count(*) as pktcnt_recev 						-- count number of times specific number of bytes occured between source and destination ips
from ipfix_enriched
group by dst_ip,packets
distributed by (ip)
;


--
-- rank distinct packets by their frequency sent by IP
--

drop table if exists frequency_stats_pkts_sent_2;
create table frequency_stats_pkts_sent_2 as 
select ip,
       packets_sent, 
       pktcnt_sent,
       ROW_NUMBER() OVER(PARTITION BY ip ORDER BY pktcnt_sent DESC,packets_sent) AS  pktrank_sent			-- rank distinct bytes by their number of times occurence between source and destination ips 
from frequency_stats_pkts_sent_1
Distributed by (ip);

--
-- rank distinct packets by their frequency received by IP
--

drop table if exists frequency_stats_pkts_recev_2;
create table frequency_stats_pkts_recev_2 as
select ip,
       packets_recev, 
       pktcnt_recev,
       ROW_NUMBER() OVER(PARTITION BY ip ORDER BY pktcnt_recev DESC,packets_recev) AS  pktrank_recev			-- rank distinct bytes by their number of times occurence between source and destination ips 
from frequency_stats_pkts_recev_1
Distributed by (ip);

--
-- Select Most frequent packets sent by IP
--

drop table if exists frequency_stats_pkts_sent;
create table frequency_stats_pkts_sent as
select * 
From frequency_stats_pkts_sent_2
where pktrank_sent = 1
Distributed by (ip);

--
-- Select Most frequent packets received by IP
--

Drop table if exists frequency_stats_pkts_recev;
create table frequency_stats_pkts_recev as 
select *
From frequency_stats_pkts_recev_2
where pktrank_recev = 1
Distributed by (ip);

--
-- Combine source and destination frequent packet stats to get Most frequent packet sent and received by IP
--

	
drop table if exists frequency_stats_packets;
create table frequency_stats_packets as 
select 
	case when c.ip is null then 
		d.ip
	     when d.ip is null then
		c.ip
	    else
		c.ip
	end as ip,
	coalesce(packets_sent,0) as frequent_packets_sent,coalesce(packets_recev,0) as frequent_packets_recev,
	coalesce(pktcnt_sent,0) as frequency_of_frequent_packets_sent,coalesce(pktcnt_recev,0) as frequency_of_frequent_packets_recev
from frequency_stats_pkts_sent c full outer join frequency_stats_pkts_recev d
on c.ip=d.ip
distributed by (ip)
;

--
-- count/frequency of specific duration_of_flow during IP being in source side
--

drop table if exists frequency_stats_dur_sent_1;
create table frequency_stats_dur_sent_1 as 
select	 src_ip as ip
	,duration_secs as dursecs_sent
	,count(*) as durcnt_sent 						-- count number of times specific number of bytes occured between source and destination ips
from ipfix_enriched
group by src_ip,duration_secs
distributed by (ip)
;

--
-- count/frequency of specific duration_of_flow during IP being in destination side 
--


drop table if exists frequency_stats_dur_recev_1;
create table frequency_stats_dur_recev_1 as 
select	 dst_ip as ip
	,duration_secs as dursecs_recev
	,count(*) as durcnt_recev 						-- count number of times specific number of bytes occured between source and destination ips
from ipfix_enriched
group by dst_ip,duration_secs
distributed by (ip)
;

--
-- rank distinct duration of flow by their frequency during IP in source side
--

drop table if exists frequency_stats_dur_sent_2;
create table frequency_stats_dur_sent_2 as
select ip,
       dursecs_sent, 
       durcnt_sent,
       ROW_NUMBER() OVER(PARTITION BY ip ORDER BY durcnt_sent DESC,dursecs_sent) AS  durrank_sent			-- rank distinct bytes by their number of times occurence between source and destination ips 
from frequency_stats_dur_sent_1
Distributed by (ip);

--
-- rank distinct duration of flow by their frequency during IP in destination side
--

drop table if exists frequency_stats_dur_recev_2;
create table frequency_stats_dur_recev_2 as 
select ip,
       dursecs_recev, 
       durcnt_recev,
       ROW_NUMBER() OVER(PARTITION BY ip ORDER BY durcnt_recev DESC,dursecs_recev) AS  durrank_recev			-- rank distinct bytes by their number of times occurence between source and destination ips 
from frequency_stats_dur_recev_1
Distributed by (ip);

--
-- Select Most Frequent duration of flow during IP in source side
--

Drop table if exists frequency_stats_dur_sent;
Create table frequency_stats_dur_sent as 
select * 
from  frequency_stats_dur_sent_2 
where durrank_sent = 1
Distributed by (ip);

--
-- Select Most frequent duration of flow during IP in destination side
--

drop table if exists frequency_stats_dur_recev;
Create table frequency_stats_dur_recev as 
select *
from  frequency_stats_dur_recev_2
where durrank_recev = 1
Distributed by (ip);

--
-- Combine IP being source and destination side duration of flow frequency stats
--

drop table if exists frequency_stats_durationsecs;
create table frequency_stats_durationsecs as 
select 
	case when c.ip is null then 
		d.ip
	     when d.ip is null then
		c.ip
	    else
		c.ip
	end as ip,
	coalesce(dursecs_sent,0) as frequent_duration_sent,coalesce(dursecs_recev,0) as frequent_duration_recev,
	coalesce(durcnt_sent,0) as frequency_of_frequent_duration_sent,coalesce(durcnt_recev,0) as frequency_of_frequent_duration_recev
from 	frequency_stats_dur_sent c full outer join frequency_stats_dur_recev d
on c.ip=d.ip
distributed by (ip)
;

--
-- count/frequency of specific bytespersec sent by IP
--

drop table if exists frequency_stats_bps_sent_1;
create table frequency_stats_bps_sent_1 as 
select	 src_ip as ip
	,bytespersec as bps_sent
	,count(*) as bpscnt_sent 						-- count number of times specific number of bytes occured between source and destination ips
from ipfix_enriched
group by src_ip,bytespersec
distributed by (ip)
;

--
-- count/frequency of specific bytespersec received by IP
--

drop table if exists frequency_stats_bps_recev_1;
create table frequency_stats_bps_recev_1 as 
select	 dst_ip as ip
	,bytespersec as bps_recev
	,count(*) as bpscnt_recev 						-- count number of times specific number of bytes occured between source and destination ips
from ipfix_enriched
group by dst_ip,bytespersec
distributed by (ip)
;

--
-- rank distinct bytespersec by frequency of occurrence for an IP being on source side
--

drop table if exists frequency_stats_bps_sent_2;
create table frequency_stats_bps_sent_2 as
select ip,
       bps_sent, 
       bpscnt_sent,
       ROW_NUMBER() OVER(PARTITION BY ip ORDER BY bpscnt_sent DESC,bps_sent) AS  bpsrank_sent			-- rank distinct bytes by their number of times occurence between source and destination ips 
from frequency_stats_bps_sent_1
distributed by (ip);

--
-- rank distinct bytespersec by frequency of occurrence for an IP being on destination side
--

drop table if exists frequency_stats_bps_recev_2;
create table frequency_stats_bps_recev_2 as
select ip,
       bps_recev, 
       bpscnt_recev,
       ROW_NUMBER() OVER(PARTITION BY ip ORDER BY bpscnt_recev DESC,bps_recev) AS  bpsrank_recev		-- rank distinct bytes by their number of times occurence between source and destination ips 
from frequency_stats_bps_recev_1
Distributed by (ip);

--
-- Select Most Frequent BPS sent by an IP
--

Drop table if exists frequency_stats_bps_sent;
Create table frequency_stats_bps_sent as 
select * 
from frequency_stats_bps_sent_2
where bpsrank_sent = 1
Distributed by (ip);

--
-- Select Most Frequent BPS received by an IP
--

Drop table if exists frequency_stats_bps_recev;
Create table frequency_stats_bps_recev as 
select *
from frequency_stats_bps_recev_2
where bpsrank_recev = 1
Distributed by (ip);

--
-- Combine source and destination frequency BPS stats for IP
--

drop table if exists frequency_stats_bytespersec;
create table frequency_stats_bytespersec as 
select 
	case when c.ip is null then 
		d.ip
	     when d.ip is null then
		c.ip
	    else
		c.ip
	end as ip,
	coalesce(bps_sent,0) as frequent_bps_sent,coalesce(bps_recev,0) as frequent_bps_recev,
	coalesce(bpscnt_sent,0) as frequency_of_frequent_bps_sent,coalesce(bpscnt_recev,0) as frequency_of_frequent_bps_recev
from frequency_stats_bps_sent c full outer join frequency_stats_bps_recev d
on c.ip=d.ip
distributed by (ip)
;

--
--  Calculate difference between current and previous row flow start timestamps (time interval) for an IP being on source side
--

Drop table if exists frequency_communication_frequency_table_srcip_1;
Create table frequency_communication_frequency_table_srcip_1 as 
select
	src_ip as ip,flow_start_ts,
	flow_start_ts - lag(flow_start_ts,1) OVER ( partition by src_ip order by src_ip,flow_start_ts) as running_freq_time		 
from ipfix_enriched
Distributed by (ip);

--
-- convert interval to seconds
--

Drop table if exists frequency_communication_frequency_table_srcip_2;
Create table frequency_communication_frequency_table_srcip_2 as 
select 
	ip,flow_start_ts,running_freq_time,
	extract(day from running_freq_time)*24*3600+extract(hour from running_freq_time)*3600 + extract(minute from running_freq_time)*60 + extract(second from running_freq_time)  as running_freq_secs  -- convert to seconds
From frequency_communication_frequency_table_srcip_1
Distributed by (ip);

--
-- count the number of times specific communication frequency occurred for an IP being on source side
--

Drop table if exists frequency_communication_frequency_table_srcip_3;
Create table frequency_communication_frequency_table_srcip_3 as 
select  
	ip,flow_start_ts,running_freq_time,
	running_freq_secs,count(*) over (partition by ip,running_freq_secs) as fsecscnt					-- count number of times specific minutes of tranmission occured between source and destination ips
From frequency_communication_frequency_table_srcip_2
Distributed by (ip);

--
-- rank the frequency of communication frequency for an IP being on source side
--

Drop table if exists frequency_communication_frequency_table_srcip_4;
Create table frequency_communication_frequency_table_srcip_4 as
SELECT    
	ip,flow_start_ts,running_freq_time,
	coalesce(running_freq_secs,0) as running_freq_secs_sent,
	fsecscnt as fsecscnt_sent,
	ROW_NUMBER() OVER(PARTITION BY ip ORDER BY fsecscnt DESC,running_freq_secs) AS fsecrank_sent  			-- rank distinct minutes by their number of times occurence between source and destination ips 	
From frequency_communication_frequency_table_srcip_3
Distributed by (ip);

--
-- Select Most frequent communication frequency for an IP being on source side
--

Drop table if exists frequency_communication_frequency_table_srcip;
Create table frequency_communication_frequency_table_srcip as 
Select *
From frequency_communication_frequency_table_srcip_4
Where fsecrank_sent = 1
Distributed  by (ip);

--
-- Calculate difference between current and previous row flow start timestamps (time interval) for an IP being on destination side
--

Drop table if exists frequency_communication_frequency_table_dstip_1;
Create table frequency_communication_frequency_table_dstip_1 as 
select
	dst_ip as ip,flow_start_ts,
	flow_start_ts - lag(flow_start_ts,1) OVER ( partition by dst_ip order by dst_ip,flow_start_ts) running_freq_time		-- difference between current and previous time with in specific source and dest ip
from ipfix_enriched
Distributed by (ip);

--
-- convert interval to seconds
--

Drop table if exists frequency_communication_frequency_table_dstip_2;
Create table frequency_communication_frequency_table_dstip_2 as 
select
	ip,flow_start_ts,running_freq_time,
	extract(day from running_freq_time)*24*3600+extract(hour from running_freq_time)*3600 + extract(minute from running_freq_time)*60 + extract(second from running_freq_time)  as running_freq_secs  -- convert to minutes
From frequency_communication_frequency_table_dstip_1
Distributed by (ip);

--
-- count the number of times specific communication frequency occurred for an IP being on destination side  
--

Drop table if exists frequency_communication_frequency_table_dstip_3;
Create table frequency_communication_frequency_table_dstip_3 as 
select  
	ip,flow_start_ts,running_freq_time,
	running_freq_secs,count(*) over (partition by ip,running_freq_secs) as fsecscnt					-- count number of times specific minutes of tranmission occured between source and destination ips
From frequency_communication_frequency_table_dstip_2
Distributed by (ip);

--
-- rank the frequency of communication frequency for an IP being on destination side  
--

Drop table if exists frequency_communication_frequency_table_dstip_4;
Create table frequency_communication_frequency_table_dstip_4 as 
SELECT    
	ip,
	coalesce(running_freq_secs,0) as running_freq_secs_recev,
	fsecscnt as fsecscnt_recev,
	ROW_NUMBER() OVER(PARTITION BY ip ORDER BY fsecscnt DESC,running_freq_secs) AS fsecrank_recev  			-- rank distinct minutes by their number of times occurence between source and destination ips 	
from frequency_communication_frequency_table_dstip_3
Distributed by (ip);

--
-- select Most frequent communication frequency for an IP being on destination side 
--

Drop table if exists frequency_communication_frequency_table_dstip;
Create table frequency_communication_frequency_table_dstip as 
Select * 
from frequency_communication_frequency_table_dstip_4
Where  fsecrank_recev = 1
Distributed by (ip);
       
--
-- combine source and destination most frequent communication frequency stats for an IP.
--

drop table if exists frequency_communication_frequency_table;
create table frequency_communication_frequency_table as 
select 
	case when e.ip is null then 
		f.ip
	     when f.ip is null then
		e.ip
	     else
		e.ip
	end as ip,
	coalesce(running_freq_secs_sent,0) as frequent_communucation_frequency_sent,
	coalesce(fsecscnt_sent,0) as frequency_of_frequent_communication_frequency_sent,
	coalesce(running_freq_secs_recev,0) as frequent_communucation_frequency_recev,
	coalesce(fsecscnt_recev,0) as frequency_of_frequent_communication_frequency_recev
from  frequency_communication_frequency_table_srcip e full outer join frequency_communication_frequency_table_dstip f
on e.ip=f.ip
distributed by (ip)
;

--
-- Aggregate on Source IP , calculate Mean, Stddev,range,IQR,median,min,max,range of time interval of communication frequency
--

drop table if exists communication_frequency_aggr_scrip;
create table communication_frequency_aggr_scrip as 
select 
	ip,
	avg(running_freq_secs)         as avgrfss_sent ,   stddev_pop(running_freq_secs)        as stdrfss_sent ,
	percentile_cont(.75) within group (order by running_freq_secs) - percentile_cont(.25) within group (order by running_freq_secs)  as iqrrfss_sent,
	max(running_freq_secs) as maxrfss_sent,
	min(running_freq_secs) as minrfss_sent,
	max(running_freq_secs)   - min(running_freq_secs)  as rangerfss_sent,
	median(running_freq_secs) as medrfss_sent
from frequency_communication_frequency_table_srcip_2
group by ip
distributed by (ip);

--
-- Aggregate on destination  IP , calculate Mean, Stddev,range,IQR,median,min,max,range of time interval of communication frequency
--

drop table if exists communication_frequency_aggr_dstip;
create table communication_frequency_aggr_dstip as 
select 
	ip,
	avg(running_freq_secs)         as avgrfss_recev ,   stddev_pop(running_freq_secs)        as stdrfss_recev ,
	percentile_cont(.75) within group (order by running_freq_secs) - percentile_cont(.25) within group (order by running_freq_secs)  as iqrrfss_recev,
	max(running_freq_secs) as maxrfss_recev,
	min(running_freq_secs) as minrfss_recev,
	max(running_freq_secs)   - min(running_freq_secs)  as rangerfss_recev,
	median(running_freq_secs) as medrfss_recev
from frequency_communication_frequency_table_dstip_2
group by ip
distributed by (ip);

--
-- combine source and destination IP, to get time interval stats for an IP while sending and receiving 
--

drop table if exists communication_frequency_aggr;
create table communication_frequency_aggr as 
select  case when a.ip is null then 
		b.ip
	     when b.ip is null then
	        a.ip
	     else
	        a.ip
	end as ip,    
	coalesce(avgrfss_sent,0)  as avgrfss_sent ,   
	coalesce(stdrfss_sent,0)  as stdrfss_sent ,
	coalesce(iqrrfss_sent,0) as iqrrfss_sent,
	coalesce(maxrfss_sent,0) as maxrfss_sent,
	coalesce(minrfss_sent,0) as minrfss_sent,
	coalesce(rangerfss_sent,0) as rangerfss_sent,
	coalesce(medrfss_sent,0) as medrfss_sent,
	coalesce(avgrfss_recev,0)  as avgrfss_recev ,   
	coalesce(stdrfss_recev,0)  as stdrfss_recev ,
	coalesce(iqrrfss_recev,0) as iqrrfss_recev,
	coalesce(maxrfss_recev,0) as maxrfss_recev,
	coalesce(minrfss_recev,0) as minrfss_recev,
	coalesce(rangerfss_recev,0) as rangerfss_recev,
	coalesce(medrfss_recev,0) as medrfss_recev
from communication_frequency_aggr_scrip a full outer join communication_frequency_aggr_dstip b
on a.ip=b.ip
distributed by (ip);

--
-- Combine/merge all frequency tables (bytes, packets, duration of flow, bytespersec) and create one frequency table.
--

 
drop table if exists frequency_stats;
create table frequency_stats as 
select 
	a.ip
       ,frequent_bytes_sent
       ,frequent_bytes_recev
       ,frequent_packets_sent
       ,frequent_packets_recev
       ,frequent_duration_sent
       ,frequent_duration_recev
       ,frequent_bps_sent
       ,frequent_bps_recev

       ,frequency_of_frequent_bytes_sent
       ,frequency_of_frequent_bytes_recev
       ,frequency_of_frequent_packets_sent
       ,frequency_of_frequent_packets_recev
       ,frequency_of_frequent_duration_sent
       ,frequency_of_frequent_duration_recev
       ,frequency_of_frequent_bps_sent
       ,frequency_of_frequent_bps_recev

       ,frequent_communucation_frequency_sent
       ,frequent_communucation_frequency_recev
       ,frequency_of_frequent_communication_frequency_sent
       ,frequency_of_frequent_communication_frequency_recev
      
from
	frequency_stats_bytes a
       ,frequency_stats_packets b
       ,frequency_stats_durationsecs c
       ,frequency_stats_bytespersec d
       ,frequency_communication_frequency_table e
where a.ip=b.ip and b.ip=c.ip and c.ip=d.ip and d.ip=e.ip
distributed by (ip)
;

--
-- Combine/Merge netowrk traffic aggregation, frequency stats and MAD stat tables and create one table  
--

drop table if exists network_traffic_behaviour_stats_1;
create table network_traffic_behaviour_stats_1 as 
select
        a.*
       ,frequent_bytes_sent
       ,frequent_bytes_recev
       ,frequent_packets_sent
       ,frequent_packets_recev
       ,frequent_duration_sent
       ,frequent_duration_recev
       ,frequent_bps_sent
       ,frequent_bps_recev
       ,frequency_of_frequent_bytes_sent
       ,frequency_of_frequent_bytes_recev
       ,frequency_of_frequent_packets_sent
       ,frequency_of_frequent_packets_recev
       ,frequency_of_frequent_duration_sent
       ,frequency_of_frequent_duration_recev
       ,frequency_of_frequent_bps_sent
       ,frequency_of_frequent_bps_recev
       ,frequent_communucation_frequency_sent
       ,frequent_communucation_frequency_recev
       ,frequency_of_frequent_communication_frequency_sent
       ,frequency_of_frequent_communication_frequency_recev

       ,madbytes_sent
       ,madbytes_recev
       ,madpkts_sent
       ,madpkts_recev
       ,maddur_sent
       ,maddur_recev
       ,madbps_sent
       ,madbps_recev

       ,avgrfss_sent  
       ,stdrfss_sent
       ,iqrrfss_sent
       ,maxrfss_sent
       ,minrfss_sent
       ,rangerfss_sent
       ,medrfss_sent
       ,avgrfss_recev  
       ,stdrfss_recev
       ,iqrrfss_recev
       ,maxrfss_recev
       ,minrfss_recev
       ,rangerfss_recev
       ,medrfss_recev
from
	network_traffic_aggr a,
	frequency_stats b,
	network_traffic_mad_stats c,
	communication_frequency_aggr d
	 
where a.ip = b.ip and b.ip = c.ip and c.ip = d.ip
distributed by (ip)
;

--
-- Get label (0 - Normal , 1 - C&C) for aggregated table by joining with ts_ip_ioc which has C&C ips.
--

drop table if exists network_traffic_behaviour_stats;
create table network_traffic_behaviour_stats as 
select  a.*, 
        case when b.srcip is null  then 
             0 
        else 
             1 
        end as label
from network_traffic_behaviour_stats_1 a left outer join (select distinct srcip from ts_ip_ioc) b
on a.ip = b.srcip  
distributed by (ip)
;


--
-- Compute Average and Standard deviation of each variable. This is to use in calculating z-score in next step 
--

drop table if exists network_traffic_behaviour_stats_aggr;
create table network_traffic_behaviour_stats_aggr as 
select 

--	compute average and standard deviation of all variables across the table  

	avg(avgpkts_sent) as aavgpkts_sent,stddev_pop(avgpkts_sent) as savgpkts_sent
	,avg(stdpkts_sent) as astdpkts_sent,stddev_pop(stdpkts_sent) as sstdpkts_sent
	,avg(avgbytes_sent) as aavgbytes_sent,stddev_pop(avgbytes_sent) as savgbytes_sent
	,avg(stdbytes_sent) as astdbytes_sent,stddev_pop(stdbytes_sent) as sstdbytes_sent
	,avg(avgdur_sent) as aavgdur_sent,stddev_pop(avgdur_sent) as savgdur_sent
	,avg(stddur_sent) as astddur_sent,stddev_pop(stddur_sent) as sstddur_sent
	,avg(avgbps_sent) as aavgbps_sent,stddev_pop(avgbps_sent) as savgbps_sent
	,avg(stdbps_sent) as astdbps_sent,stddev_pop(stdbps_sent) as sstdbps_sent
	,avg(count_distinct_dst_ports_sent) as acount_distinct_dst_ports_sent,stddev_pop(count_distinct_dst_ports_sent) as scount_distinct_dst_ports_sent
	,avg(count_distinct_source_ports_sent) as acount_distinct_source_ports_sent,stddev_pop(count_distinct_source_ports_sent) as scount_distinct_source_ports_sent
	,avg(count_of_flows_sent) as acount_of_flows_sent,stddev_pop(count_of_flows_sent) as scount_of_flows_sent
	,avg(iqrbytes_sent) as aiqrbytes_sent,stddev_pop(iqrbytes_sent) as siqrbytes_sent
	,avg(iqrpkts_sent) as aiqrpkts_sent,stddev_pop(iqrpkts_sent) as siqrpkts_sent
	,avg(iqrdur_sent) as aiqrdur_sent,stddev_pop(iqrdur_sent) as siqrdur_sent
	,avg(iqrbps_sent) as aiqrbps_sent,stddev_pop(iqrbps_sent) as siqrbps_sent
	,avg(minpkts_sent) as aminpkts_sent,stddev_pop(minpkts_sent) as sminpkts_sent
	,avg(maxpkts_sent) as amaxpkts_sent,stddev_pop(maxpkts_sent) as smaxpkts_sent
	,avg(minbytes_sent) as aminbytes_sent,stddev_pop(minbytes_sent) as sminbytes_sent
	,avg(maxbytes_sent) as amaxbytes_sent,stddev_pop(maxbytes_sent) as smaxbytes_sent
	,avg(mindur_sent) as amindur_sent,stddev_pop(mindur_sent) as smindur_sent
	,avg(maxdur_sent) as amaxdur_sent,stddev_pop(maxdur_sent) as smaxdur_sent
	,avg(minbps_sent) as aminbps_sent,stddev_pop(minbps_sent) as sminbps_sent
	,avg(maxbps_sent) as amaxbps_sent,stddev_pop(maxbps_sent) as smaxbps_sent
	,avg(rangepkts_sent) as arangepkts_sent,stddev_pop(rangepkts_sent) as srangepkts_sent
	,avg(rangebytes_sent) as arangebytes_sent,stddev_pop(rangebytes_sent) as srangebytes_sent
	,avg(rangedur_sent) as arangedur_sent,stddev_pop(rangedur_sent) as srangedur_sent
	,avg(rangebps_sent) as arangebps_sent,stddev_pop(rangebps_sent) as srangebps_sent
	,avg(medbytes_sent) as amedbytes_sent,stddev_pop(medbytes_sent) as smedbytes_sent
	,avg(medpkts_sent) as amedpkts_sent,stddev_pop(medpkts_sent) as smedpkts_sent
	,avg(meddur_sent) as ameddur_sent,stddev_pop(meddur_sent) as smeddur_sent
	,avg(medbps_sent) as amedbps_sent,stddev_pop(medbps_sent) as smedbps_sent
	,avg(avgpkts_recev) as aavgpkts_recev,stddev_pop(avgpkts_recev) as savgpkts_recev
	,avg(stdpkts_recev) as astdpkts_recev,stddev_pop(stdpkts_recev) as sstdpkts_recev
	,avg(avgbytes_recev) as aavgbytes_recev,stddev_pop(avgbytes_recev) as savgbytes_recev
	,avg(stdbytes_recev) as astdbytes_recev,stddev_pop(stdbytes_recev) as sstdbytes_recev
	,avg(avgdur_recev) as aavgdur_recev,stddev_pop(avgdur_recev) as savgdur_recev
	,avg(stddur_recev) as astddur_recev,stddev_pop(stddur_recev) as sstddur_recev
	,avg(avgbps_recev) as aavgbps_recev,stddev_pop(avgbps_recev) as savgbps_recev
	,avg(stdbps_recev) as astdbps_recev,stddev_pop(stdbps_recev) as sstdbps_recev
	,avg(count_distinct_dst_ports_recev) as acount_distinct_dst_ports_recev,stddev_pop(count_distinct_dst_ports_recev) as scount_distinct_dst_ports_recev
	,avg(count_distinct_source_ports_recev) as acount_distinct_source_ports_recev,stddev_pop(count_distinct_source_ports_recev) as scount_distinct_source_ports_recev
	,avg(count_of_flows_recev) as acount_of_flows_recev,stddev_pop(count_of_flows_recev) as scount_of_flows_recev
	,avg(iqrbytes_recev) as aiqrbytes_recev,stddev_pop(iqrbytes_recev) as siqrbytes_recev
	,avg(iqrpkts_recev) as aiqrpkts_recev,stddev_pop(iqrpkts_recev) as siqrpkts_recev
	,avg(iqrdur_recev) as aiqrdur_recev,stddev_pop(iqrdur_recev) as siqrdur_recev
	,avg(iqrbps_recev) as aiqrbps_recev,stddev_pop(iqrbps_recev) as siqrbps_recev
	,avg(minpkts_recev) as aminpkts_recev,stddev_pop(minpkts_recev) as sminpkts_recev
	,avg(maxpkts_recev) as amaxpkts_recev,stddev_pop(maxpkts_recev) as smaxpkts_recev
	,avg(minbytes_recev) as aminbytes_recev,stddev_pop(minbytes_recev) as sminbytes_recev
	,avg(maxbytes_recev) as amaxbytes_recev,stddev_pop(maxbytes_recev) as smaxbytes_recev
	,avg(mindur_recev) as amindur_recev,stddev_pop(mindur_recev) as smindur_recev
	,avg(maxdur_recev) as amaxdur_recev,stddev_pop(maxdur_recev) as smaxdur_recev
	,avg(minbps_recev) as aminbps_recev,stddev_pop(minbps_recev) as sminbps_recev
	,avg(maxbps_recev) as amaxbps_recev,stddev_pop(maxbps_recev) as smaxbps_recev
	,avg(rangepkts_recev) as arangepkts_recev,stddev_pop(rangepkts_recev) as srangepkts_recev
	,avg(rangebytes_recev) as arangebytes_recev,stddev_pop(rangebytes_recev) as srangebytes_recev
	,avg(rangedur_recev) as arangedur_recev,stddev_pop(rangedur_recev) as srangedur_recev
	,avg(rangebps_recev) as arangebps_recev,stddev_pop(rangebps_recev) as srangebps_recev
	,avg(medbytes_recev) as amedbytes_recev,stddev_pop(medbytes_recev) as smedbytes_recev
	,avg(medpkts_recev) as amedpkts_recev,stddev_pop(medpkts_recev) as smedpkts_recev
	,avg(meddur_recev) as ameddur_recev,stddev_pop(meddur_recev) as smeddur_recev
	,avg(medbps_recev) as amedbps_recev,stddev_pop(medbps_recev) as smedbps_recev
	,avg(frequent_bytes_sent) as afrequent_bytes_sent,stddev_pop(frequent_bytes_sent) as sfrequent_bytes_sent
	,avg(frequent_bytes_recev) as afrequent_bytes_recev,stddev_pop(frequent_bytes_recev) as sfrequent_bytes_recev
	,avg(frequent_packets_sent) as afrequent_packets_sent,stddev_pop(frequent_packets_sent) as sfrequent_packets_sent
	,avg(frequent_packets_recev) as afrequent_packets_recev,stddev_pop(frequent_packets_recev) as sfrequent_packets_recev
	,avg(frequent_duration_sent) as afrequent_duration_sent,stddev_pop(frequent_duration_sent) as sfrequent_duration_sent
	,avg(frequent_duration_recev) as afrequent_duration_recev,stddev_pop(frequent_duration_recev) as sfrequent_duration_recev
	,avg(frequent_bps_sent) as afrequent_bps_sent,stddev_pop(frequent_bps_sent) as sfrequent_bps_sent
	,avg(frequent_bps_recev) as afrequent_bps_recev,stddev_pop(frequent_bps_recev) as sfrequent_bps_recev
	,avg(frequency_of_frequent_bytes_sent) as afrequency_of_frequent_bytes_sent,stddev_pop(frequency_of_frequent_bytes_sent) as sfrequency_of_frequent_bytes_sent
	,avg(frequency_of_frequent_bytes_recev) as afrequency_of_frequent_bytes_recev,stddev_pop(frequency_of_frequent_bytes_recev) as sfrequency_of_frequent_bytes_recev
	,avg(frequency_of_frequent_packets_sent) as afrequency_of_frequent_packets_sent,stddev_pop(frequency_of_frequent_packets_sent) as sfrequency_of_frequent_packets_sent
	,avg(frequency_of_frequent_packets_recev) as afrequency_of_frequent_packets_recev,stddev_pop(frequency_of_frequent_packets_recev) as sfrequency_of_frequent_packets_recev
	,avg(frequency_of_frequent_duration_sent) as afrequency_of_frequent_duration_sent,stddev_pop(frequency_of_frequent_duration_sent) as sfrequency_of_frequent_duration_sent
	,avg(frequency_of_frequent_duration_recev) as afrequency_of_frequent_duration_recev,stddev_pop(frequency_of_frequent_duration_recev) as sfrequency_of_frequent_duration_recev
	,avg(frequency_of_frequent_bps_sent) as afrequency_of_frequent_bps_sent,stddev_pop(frequency_of_frequent_bps_sent) as sfrequency_of_frequent_bps_sent
	,avg(frequency_of_frequent_bps_recev) as afrequency_of_frequent_bps_recev,stddev_pop(frequency_of_frequent_bps_recev) as sfrequency_of_frequent_bps_recev
	,avg(frequent_communucation_frequency_sent) as afrequent_communucation_frequency_sent,stddev_pop(frequent_communucation_frequency_sent) as sfrequent_communucation_frequency_sent
	,avg(frequent_communucation_frequency_recev) as afrequent_communucation_frequency_recev,stddev_pop(frequent_communucation_frequency_recev) as sfrequent_communucation_frequency_recev
	,avg(frequency_of_frequent_communication_frequency_sent) as afrequency_of_frequent_communication_frequency_sent,stddev_pop(frequency_of_frequent_communication_frequency_sent) as sfrequency_of_frequent_communication_frequency_sent
	,avg(frequency_of_frequent_communication_frequency_recev) as afrequency_of_frequent_communication_frequency_recev,stddev_pop(frequency_of_frequent_communication_frequency_recev) as sfrequency_of_frequent_communication_frequency_recev
	,avg(madbytes_sent) as amadbytes_sent,stddev_pop(madbytes_sent) as smadbytes_sent
	,avg(madbytes_recev) as amadbytes_recev,stddev_pop(madbytes_recev) as smadbytes_recev
	,avg(madpkts_sent) as amadpkts_sent,stddev_pop(madpkts_sent) as smadpkts_sent
	,avg(madpkts_recev) as amadpkts_recev,stddev_pop(madpkts_recev) as smadpkts_recev
	,avg(maddur_sent) as amaddur_sent,stddev_pop(maddur_sent) as smaddur_sent
	,avg(maddur_recev) as amaddur_recev,stddev_pop(maddur_recev) as smaddur_recev
	,avg(madbps_sent) as amadbps_sent,stddev_pop(madbps_sent) as smadbps_sent
	,avg(madbps_recev) as amadbps_recev,stddev_pop(madbps_recev) as smadbps_recev

        ,avg(avgrfss_sent) as aavgrfss_sent,stddev_pop(avgrfss_sent) as savgrfss_sent 
        ,avg(stdrfss_sent) as astdrfss_sent,stddev_pop(stdrfss_sent) as sstdrfss_sent
        ,avg(iqrrfss_sent) as aiqrrfss_sent,stddev_pop(iqrrfss_sent) as siqrrfss_sent
        ,avg(maxrfss_sent) as amaxrfss_sent,stddev_pop(maxrfss_sent) as smaxrfss_sent
        ,avg(minrfss_sent) as aminrfss_sent,stddev_pop(minrfss_sent) as sminrfss_sent
        ,avg(rangerfss_sent) as arangerfss_sent,stddev_pop(rangerfss_sent) as srangerfss_sent
        ,avg(medrfss_sent) as amedrfss_sent,stddev_pop(medrfss_sent) as smedrfss_sent

        ,avg(avgrfss_recev) as aavgrfss_recev,stddev_pop(avgrfss_recev) as savgrfss_recev  
        ,avg(stdrfss_recev) as astdrfss_recev,stddev_pop(stdrfss_recev) as sstdrfss_recev
        ,avg(iqrrfss_recev) as aiqrrfss_recev,stddev_pop(iqrrfss_recev) as siqrrfss_recev
        ,avg(maxrfss_recev) as amaxrfss_recev,stddev_pop(maxrfss_recev) as smaxrfss_recev
        ,avg(minrfss_recev) as aminrfss_recev,stddev_pop(minrfss_recev) as sminrfss_recev
        ,avg(rangerfss_recev) as arangerfss_recev,stddev_pop(rangerfss_recev) as srangerfss_recev
        ,avg(medrfss_recev) as amedrfss_recev,stddev_pop(medrfss_recev) as smedrfss_recev
        ,avg(in_degree) as ain_degree,stddev_pop(in_degree) as sin_degree
        ,avg(out_degree) as aout_degree,stddev_pop(out_degree) as sout_degree
        ,avg(in_out_ratio) as ain_out_ratio,stddev_pop(in_out_ratio) as sin_out_ratio
        ,avg(pct_dst_ports_flows_per_ip_sent) as apct_dst_ports_flows_per_ip_sent,stddev_pop(pct_dst_ports_flows_per_ip_sent) as spct_dst_ports_flows_per_ip_sent
        ,avg(pct_source_ports_flows_per_ip_sent) as apct_source_ports_flows_per_ip_sent,stddev_pop(pct_source_ports_flows_per_ip_sent) as spct_source_ports_flows_per_ip_sent
        ,avg(pct_dst_ports_flows_per_ip_recev) as apct_dst_ports_flows_per_ip_recev,stddev_pop(pct_dst_ports_flows_per_ip_recev) as spct_dst_ports_flows_per_ip_recev
        ,avg(pct_source_ports_flows_per_ip_recev) as apct_source_ports_flows_per_ip_recev,stddev_pop(pct_source_ports_flows_per_ip_recev) as spct_source_ports_flows_per_ip_recev

       
from network_traffic_behaviour_stats;

--
-- Standardize data for faster convergence and reduced feature domination
--
 
drop table if exists network_traffic_behaviour_stats_standardized_stage1;
create table network_traffic_behaviour_stats_standardized_stage1 as
select 
         ip
	,(avgpkts_sent-aavgpkts_sent)/savgpkts_sent as avgpkts_sent
	,(stdpkts_sent-astdpkts_sent)/sstdpkts_sent as stdpkts_sent
	,(avgbytes_sent-aavgbytes_sent)/savgbytes_sent as avgbytes_sent
	,(stdbytes_sent-astdbytes_sent)/sstdbytes_sent as stdbytes_sent
	,(avgdur_sent-aavgdur_sent)/savgdur_sent as avgdur_sent
	,(stddur_sent-astddur_sent)/sstddur_sent as stddur_sent
	,(avgbps_sent-aavgbps_sent)/savgbps_sent as avgbps_sent
	,(stdbps_sent-astdbps_sent)/sstdbps_sent as stdbps_sent
	,(count_distinct_dst_ports_sent-acount_distinct_dst_ports_sent)/scount_distinct_dst_ports_sent as count_distinct_dst_ports_sent
	,(count_distinct_source_ports_sent-acount_distinct_source_ports_sent)/scount_distinct_source_ports_sent as count_distinct_source_ports_sent
	,(count_of_flows_sent-acount_of_flows_sent)/scount_of_flows_sent as count_of_flows_sent
	,(iqrbytes_sent-aiqrbytes_sent)/siqrbytes_sent as iqrbytes_sent
	,(iqrpkts_sent-aiqrpkts_sent)/siqrpkts_sent as iqrpkts_sent
	,(iqrdur_sent-aiqrdur_sent)/siqrdur_sent as iqrdur_sent
	,(iqrbps_sent-aiqrbps_sent)/siqrbps_sent as iqrbps_sent
	,(minpkts_sent-aminpkts_sent)/sminpkts_sent as minpkts_sent
	,(maxpkts_sent-amaxpkts_sent)/smaxpkts_sent as maxpkts_sent
	,(minbytes_sent-aminbytes_sent)/sminbytes_sent as minbytes_sent
	,(maxbytes_sent-amaxbytes_sent)/smaxbytes_sent as maxbytes_sent
	,(mindur_sent-amindur_sent)/smindur_sent as mindur_sent
	,(maxdur_sent-amaxdur_sent)/smaxdur_sent as maxdur_sent
	,(minbps_sent-aminbps_sent)/sminbps_sent as minbps_sent
	,(maxbps_sent-amaxbps_sent)/smaxbps_sent as maxbps_sent
	,(rangepkts_sent-arangepkts_sent)/srangepkts_sent as rangepkts_sent
	,(rangebytes_sent-arangebytes_sent)/srangebytes_sent as rangebytes_sent
	,(rangedur_sent-arangedur_sent)/srangedur_sent as rangedur_sent
	,(rangebps_sent-arangebps_sent)/srangebps_sent as rangebps_sent
	,(medbytes_sent-amedbytes_sent)/smedbytes_sent as medbytes_sent
	,(medpkts_sent-amedpkts_sent)/smedpkts_sent as medpkts_sent
	,(meddur_sent-ameddur_sent)/smeddur_sent as meddur_sent
	,(medbps_sent-amedbps_sent)/smedbps_sent as medbps_sent
	,(avgpkts_recev-aavgpkts_recev)/savgpkts_recev as avgpkts_recev
	,(stdpkts_recev-astdpkts_recev)/sstdpkts_recev as stdpkts_recev
	,(avgbytes_recev-aavgbytes_recev)/savgbytes_recev as avgbytes_recev
	,(stdbytes_recev-astdbytes_recev)/sstdbytes_recev as stdbytes_recev
	,(avgdur_recev-aavgdur_recev)/savgdur_recev as avgdur_recev
	,(stddur_recev-astddur_recev)/sstddur_recev as stddur_recev
	,(avgbps_recev-aavgbps_recev)/savgbps_recev as avgbps_recev
	,(stdbps_recev-astdbps_recev)/sstdbps_recev as stdbps_recev
	,(count_distinct_dst_ports_recev-acount_distinct_dst_ports_recev)/scount_distinct_dst_ports_recev as count_distinct_dst_ports_recev
	,(count_distinct_source_ports_recev-acount_distinct_source_ports_recev)/scount_distinct_source_ports_recev as count_distinct_source_ports_recev
	,(count_of_flows_recev-acount_of_flows_recev)/scount_of_flows_recev as count_of_flows_recev
	,(iqrbytes_recev-aiqrbytes_recev)/siqrbytes_recev as iqrbytes_recev
	,(iqrpkts_recev-aiqrpkts_recev)/siqrpkts_recev as iqrpkts_recev
	,(iqrdur_recev-aiqrdur_recev)/siqrdur_recev as iqrdur_recev
	,(iqrbps_recev-aiqrbps_recev)/siqrbps_recev as iqrbps_recev
	,(minpkts_recev-aminpkts_recev)/sminpkts_recev as minpkts_recev
	,(maxpkts_recev-amaxpkts_recev)/smaxpkts_recev as maxpkts_recev
	,(minbytes_recev-aminbytes_recev)/sminbytes_recev as minbytes_recev
	,(maxbytes_recev-amaxbytes_recev)/smaxbytes_recev as maxbytes_recev
	,(mindur_recev-amindur_recev)/smindur_recev as mindur_recev
	,(maxdur_recev-amaxdur_recev)/smaxdur_recev as maxdur_recev
	,(minbps_recev-aminbps_recev)/sminbps_recev as minbps_recev
	,(maxbps_recev-amaxbps_recev)/smaxbps_recev as maxbps_recev
	,(rangepkts_recev-arangepkts_recev)/srangepkts_recev as rangepkts_recev
	,(rangebytes_recev-arangebytes_recev)/srangebytes_recev as rangebytes_recev
	,(rangedur_recev-arangedur_recev)/srangedur_recev as rangedur_recev
	,(rangebps_recev-arangebps_recev)/srangebps_recev as rangebps_recev
	,(medbytes_recev-amedbytes_recev)/smedbytes_recev as medbytes_recev
	,(medpkts_recev-amedpkts_recev)/smedpkts_recev as medpkts_recev
	,(meddur_recev-ameddur_recev)/smeddur_recev as meddur_recev
	,(medbps_recev-amedbps_recev)/smedbps_recev as medbps_recev
	,(frequent_bytes_sent-afrequent_bytes_sent)/sfrequent_bytes_sent as frequent_bytes_sent
	,(frequent_bytes_recev-afrequent_bytes_recev)/sfrequent_bytes_recev as frequent_bytes_recev
	,(frequent_packets_sent-afrequent_packets_sent)/sfrequent_packets_sent as frequent_packets_sent
	,(frequent_packets_recev-afrequent_packets_recev)/sfrequent_packets_recev as frequent_packets_recev
	,(frequent_duration_sent-afrequent_duration_sent)/sfrequent_duration_sent as frequent_duration_sent
	,(frequent_duration_recev-afrequent_duration_recev)/sfrequent_duration_recev as frequent_duration_recev
	,(frequent_bps_sent-afrequent_bps_sent)/sfrequent_bps_sent as frequent_bps_sent
	,(frequent_bps_recev-afrequent_bps_recev)/sfrequent_bps_recev as frequent_bps_recev
	,(frequency_of_frequent_bytes_sent-afrequency_of_frequent_bytes_sent)/sfrequency_of_frequent_bytes_sent as frequency_of_frequent_bytes_sent
	,(frequency_of_frequent_bytes_recev-afrequency_of_frequent_bytes_recev)/sfrequency_of_frequent_bytes_recev as frequency_of_frequent_bytes_recev
	,(frequency_of_frequent_packets_sent-afrequency_of_frequent_packets_sent)/sfrequency_of_frequent_packets_sent as frequency_of_frequent_packets_sent
	,(frequency_of_frequent_packets_recev-afrequency_of_frequent_packets_recev)/sfrequency_of_frequent_packets_recev as frequency_of_frequent_packets_recev
	,(frequency_of_frequent_duration_sent-afrequency_of_frequent_duration_sent)/sfrequency_of_frequent_duration_sent as frequency_of_frequent_duration_sent
	,(frequency_of_frequent_duration_recev-afrequency_of_frequent_duration_recev)/sfrequency_of_frequent_duration_recev as frequency_of_frequent_duration_recev
	,(frequency_of_frequent_bps_sent-afrequency_of_frequent_bps_sent)/sfrequency_of_frequent_bps_sent as frequency_of_frequent_bps_sent
	,(frequency_of_frequent_bps_recev-afrequency_of_frequent_bps_recev)/sfrequency_of_frequent_bps_recev as frequency_of_frequent_bps_recev
	,(frequent_communucation_frequency_sent-afrequent_communucation_frequency_sent)/sfrequent_communucation_frequency_sent as frequent_communucation_frequency_sent
	,(frequent_communucation_frequency_recev-afrequent_communucation_frequency_recev)/sfrequent_communucation_frequency_recev as frequent_communucation_frequency_recev
	,(frequency_of_frequent_communication_frequency_sent-afrequency_of_frequent_communication_frequency_sent)/sfrequency_of_frequent_communication_frequency_sent as frequency_of_frequent_communication_frequency_sent
	,(frequency_of_frequent_communication_frequency_recev-afrequency_of_frequent_communication_frequency_recev)/sfrequency_of_frequent_communication_frequency_recev as frequency_of_frequent_communication_frequency_recev
	,(madbytes_sent-amadbytes_sent)/smadbytes_sent as madbytes_sent
	,(madbytes_recev-amadbytes_recev)/smadbytes_recev as madbytes_recev
	,(madpkts_sent-amadpkts_sent)/smadpkts_sent as madpkts_sent
	,(madpkts_recev-amadpkts_recev)/smadpkts_recev as madpkts_recev
	,(maddur_sent-amaddur_sent)/smaddur_sent as maddur_sent
	,(maddur_recev-amaddur_recev)/smaddur_recev as maddur_recev
	,(madbps_sent-amadbps_sent)/smadbps_sent as madbps_sent
	,(madbps_recev-amadbps_recev)/smadbps_recev as madbps_recev
	
	,(avgrfss_sent-aavgrfss_sent)/savgrfss_sent as avgrfss_sent
        ,(stdrfss_sent-astdrfss_sent)/sstdrfss_sent as stdrfss_sent
        ,(iqrrfss_sent-aiqrrfss_sent)/siqrrfss_sent as iqrrfss_sent
        ,(maxrfss_sent-amaxrfss_sent)/smaxrfss_sent as maxrfss_sent
        ,(minrfss_sent-aminrfss_sent)/sminrfss_sent as minrfss_sent
        ,(rangerfss_sent-arangerfss_sent)/srangerfss_sent as rangerfss_sent
        ,(medrfss_sent-amedrfss_sent)/smedrfss_sent as medrfss_sent
        ,(avgrfss_recev-aavgrfss_recev)/savgrfss_recev  as avgrfss_recev
        ,(stdrfss_recev-astdrfss_recev)/sstdrfss_recev  as stdrfss_recev
        ,(iqrrfss_recev-aiqrrfss_recev)/siqrrfss_recev  as iqrrfss_recev
        ,(maxrfss_recev-amaxrfss_recev)/smaxrfss_recev  as maxrfss_recev
        ,(minrfss_recev-aminrfss_recev)/sminrfss_recev  as minrfss_recev
        ,(rangerfss_recev-arangerfss_recev)/srangerfss_recev as rangerfss_recev
        ,(medrfss_recev-amedrfss_recev)/smedrfss_recev as medrfss_recev
	,(in_degree - ain_degree)/sin_degree as in_degree
	,(out_degree - aout_degree)/sout_degree as out_degree
	,(in_out_ratio - ain_out_ratio)/sin_out_ratio as in_out_ratio
        ,(pct_dst_ports_flows_per_ip_sent-apct_dst_ports_flows_per_ip_sent)/spct_dst_ports_flows_per_ip_sent as pct_dst_ports_flows_per_ip_sent
        ,(pct_source_ports_flows_per_ip_sent-apct_source_ports_flows_per_ip_sent)/spct_source_ports_flows_per_ip_sent as pct_source_ports_flows_per_ip_sent
        ,(pct_dst_ports_flows_per_ip_recev-apct_dst_ports_flows_per_ip_recev)/spct_dst_ports_flows_per_ip_recev as pct_dst_ports_flows_per_ip_recev
        ,(pct_source_ports_flows_per_ip_recev-apct_source_ports_flows_per_ip_recev)/spct_source_ports_flows_per_ip_recev as pct_source_ports_flows_per_ip_recev

	,label
from network_traffic_behaviour_stats, network_traffic_behaviour_stats_aggr
distributed by (ip)
;
--
-- Assign unique id to each ip
--
drop table if exists network_traffic_behaviour_stats_standardized;
create table network_traffic_behaviour_stats_standardized as
select 
         row_number() over() as id,
	 *
from network_traffic_behaviour_stats_standardized_stage1
distributed randomly;
