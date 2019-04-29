Select *
from
(SELECT 'avgpkts_sent' as columnname,
       (MADlib.one_way_anova (label,avgpkts_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'stdpkts_sent' as columnname,
       (MADlib.one_way_anova (label,stdpkts_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'avgbytes_sent' as columnname,
       (MADlib.one_way_anova (label,avgbytes_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'stdbytes_sent' as columnname,
       (MADlib.one_way_anova (label,stdbytes_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'avgdur_sent' as columnname,
       (MADlib.one_way_anova (label,avgdur_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'stddur_sent' as columnname,
       (MADlib.one_way_anova (label,stddur_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'avgbps_sent' as columnname,
       (MADlib.one_way_anova (label,avgbps_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'stdbps_sent' as columnname,
       (MADlib.one_way_anova (label,stdbps_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'count_distinct_dst_ports_sent' as columnname,
       (MADlib.one_way_anova (label,count_distinct_dst_ports_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'count_distinct_source_ports_sent' as columnname,
       (MADlib.one_way_anova (label,count_distinct_source_ports_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'count_of_flows_sent' as columnname,
       (MADlib.one_way_anova (label,count_of_flows_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'iqrbytes_sent' as columnname,
       (MADlib.one_way_anova (label,iqrbytes_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'iqrpkts_sent' as columnname,
       (MADlib.one_way_anova (label,iqrpkts_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'iqrdur_sent' as columnname,
       (MADlib.one_way_anova (label,iqrdur_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'iqrbps_sent' as columnname,
       (MADlib.one_way_anova (label,iqrbps_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'minpkts_sent' as columnname,
       (MADlib.one_way_anova (label,minpkts_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'maxpkts_sent' as columnname,
       (MADlib.one_way_anova (label,maxpkts_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'minbytes_sent' as columnname,
       (MADlib.one_way_anova (label,minbytes_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'maxbytes_sent' as columnname,
       (MADlib.one_way_anova (label,maxbytes_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'mindur_sent' as columnname,
       (MADlib.one_way_anova (label,mindur_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'maxdur_sent' as columnname,
       (MADlib.one_way_anova (label,maxdur_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'minbps_sent' as columnname,
       (MADlib.one_way_anova (label,minbps_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'maxbps_sent' as columnname,
       (MADlib.one_way_anova (label,maxbps_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'rangepkts_sent' as columnname,
       (MADlib.one_way_anova (label,rangepkts_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'rangebytes_sent' as columnname,
       (MADlib.one_way_anova (label,rangebytes_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'rangedur_sent' as columnname,
       (MADlib.one_way_anova (label,rangedur_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'rangebps_sent' as columnname,
       (MADlib.one_way_anova (label,rangebps_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'medbytes_sent' as columnname,
       (MADlib.one_way_anova (label,medbytes_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'medpkts_sent' as columnname,
       (MADlib.one_way_anova (label,medpkts_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'meddur_sent' as columnname,
       (MADlib.one_way_anova (label,meddur_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'medbps_sent' as columnname,
       (MADlib.one_way_anova (label,medbps_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'avgpkts_recev' as columnname,
       (MADlib.one_way_anova (label,avgpkts_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'stdpkts_recev' as columnname,
       (MADlib.one_way_anova (label,stdpkts_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'avgbytes_recev' as columnname,
       (MADlib.one_way_anova (label,avgbytes_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'stdbytes_recev' as columnname,
       (MADlib.one_way_anova (label,stdbytes_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'avgdur_recev' as columnname,
       (MADlib.one_way_anova (label,avgdur_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'stddur_recev' as columnname,
       (MADlib.one_way_anova (label,stddur_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'avgbps_recev' as columnname,
       (MADlib.one_way_anova (label,avgbps_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'stdbps_recev' as columnname,
       (MADlib.one_way_anova (label,stdbps_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'count_distinct_dst_ports_recev' as columnname,
       (MADlib.one_way_anova (label,count_distinct_dst_ports_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'count_distinct_source_ports_recev' as columnname,
       (MADlib.one_way_anova (label,count_distinct_source_ports_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'count_of_flows_recev' as columnname,
       (MADlib.one_way_anova (label,count_of_flows_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'iqrbytes_recev' as columnname,
       (MADlib.one_way_anova (label,iqrbytes_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'iqrpkts_recev' as columnname,
       (MADlib.one_way_anova (label,iqrpkts_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'iqrdur_recev' as columnname,
       (MADlib.one_way_anova (label,iqrdur_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'iqrbps_recev' as columnname,
       (MADlib.one_way_anova (label,iqrbps_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'minpkts_recev' as columnname,
       (MADlib.one_way_anova (label,minpkts_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'maxpkts_recev' as columnname,
       (MADlib.one_way_anova (label,maxpkts_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'minbytes_recev' as columnname,
       (MADlib.one_way_anova (label,minbytes_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'maxbytes_recev' as columnname,
       (MADlib.one_way_anova (label,maxbytes_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'mindur_recev' as columnname,
       (MADlib.one_way_anova (label,mindur_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'maxdur_recev' as columnname,
       (MADlib.one_way_anova (label,maxdur_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'minbps_recev' as columnname,
       (MADlib.one_way_anova (label,minbps_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'maxbps_recev' as columnname,
       (MADlib.one_way_anova (label,maxbps_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'rangepkts_recev' as columnname,
       (MADlib.one_way_anova (label,rangepkts_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'rangebytes_recev' as columnname,
       (MADlib.one_way_anova (label,rangebytes_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'rangedur_recev' as columnname,
       (MADlib.one_way_anova (label,rangedur_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'rangebps_recev' as columnname,
       (MADlib.one_way_anova (label,rangebps_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'medbytes_recev' as columnname,
       (MADlib.one_way_anova (label,medbytes_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'medpkts_recev' as columnname,
       (MADlib.one_way_anova (label,medpkts_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'meddur_recev' as columnname,
       (MADlib.one_way_anova (label,meddur_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'medbps_recev' as columnname,
       (MADlib.one_way_anova (label,medbps_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'in_out_ratio' as columnname,
       (MADlib.one_way_anova (label,in_out_ratio)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'in_degree' as columnname,
       (MADlib.one_way_anova (label,in_degree)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'out_degree' as columnname,
       (MADlib.one_way_anova (label,out_degree)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequent_bytes_sent' as columnname,
       (MADlib.one_way_anova (label,frequent_bytes_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequent_bytes_recev' as columnname,
       (MADlib.one_way_anova (label,frequent_bytes_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequent_packets_sent' as columnname,
       (MADlib.one_way_anova (label,frequent_packets_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequent_packets_recev' as columnname,
       (MADlib.one_way_anova (label,frequent_packets_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequent_duration_sent' as columnname,
       (MADlib.one_way_anova (label,frequent_duration_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequent_duration_recev' as columnname,
       (MADlib.one_way_anova (label,frequent_duration_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequent_bps_sent' as columnname,
       (MADlib.one_way_anova (label,frequent_bps_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequent_bps_recev' as columnname,
       (MADlib.one_way_anova (label,frequent_bps_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequency_of_frequent_bytes_sent' as columnname,
       (MADlib.one_way_anova (label,frequency_of_frequent_bytes_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequency_of_frequent_bytes_recev' as columnname,
       (MADlib.one_way_anova (label,frequency_of_frequent_bytes_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequency_of_frequent_packets_sent' as columnname,
       (MADlib.one_way_anova (label,frequency_of_frequent_packets_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequency_of_frequent_packets_recev' as columnname,
       (MADlib.one_way_anova (label,frequency_of_frequent_packets_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequency_of_frequent_duration_sent' as columnname,
       (MADlib.one_way_anova (label,frequency_of_frequent_duration_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequency_of_frequent_duration_recev' as columnname,
       (MADlib.one_way_anova (label,frequency_of_frequent_duration_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequency_of_frequent_bps_sent' as columnname,
       (MADlib.one_way_anova (label,frequency_of_frequent_bps_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequency_of_frequent_bps_recev' as columnname,
       (MADlib.one_way_anova (label,frequency_of_frequent_bps_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequent_communucation_frequency_sent' as columnname,
       (MADlib.one_way_anova (label,frequent_communucation_frequency_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequent_communucation_frequency_recev' as columnname,
       (MADlib.one_way_anova (label,frequent_communucation_frequency_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequency_of_frequent_communication_frequency_sent' as columnname,
       (MADlib.one_way_anova (label,frequency_of_frequent_communication_frequency_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'frequency_of_frequent_communication_frequency_recev' as columnname,
       (MADlib.one_way_anova (label,frequency_of_frequent_communication_frequency_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'madbytes_sent' as columnname,
       (MADlib.one_way_anova (label,madbytes_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'madbytes_recev' as columnname,
       (MADlib.one_way_anova (label,madbytes_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'madpkts_sent' as columnname,
       (MADlib.one_way_anova (label,madpkts_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'madpkts_recev' as columnname,
       (MADlib.one_way_anova (label,madpkts_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'maddur_sent' as columnname,
       (MADlib.one_way_anova (label,maddur_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'maddur_recev' as columnname,
       (MADlib.one_way_anova (label,maddur_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'madbps_sent' as columnname,
       (MADlib.one_way_anova (label,madbps_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'madbps_recev' as columnname,
       (MADlib.one_way_anova (label,madbps_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'avgrfss_sent' as columnname,
       (MADlib.one_way_anova (label,avgrfss_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'stdrfss_sent' as columnname,
       (MADlib.one_way_anova (label,stdrfss_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'iqrrfss_sent' as columnname,
       (MADlib.one_way_anova (label,iqrrfss_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'maxrfss_sent' as columnname,
       (MADlib.one_way_anova (label,maxrfss_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'minrfss_sent' as columnname,
       (MADlib.one_way_anova (label,minrfss_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'rangerfss_sent' as columnname,
       (MADlib.one_way_anova (label,rangerfss_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'medrfss_sent' as columnname,
       (MADlib.one_way_anova (label,medrfss_sent)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'avgrfss_recev' as columnname,
       (MADlib.one_way_anova (label,avgrfss_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'stdrfss_recev' as columnname,
       (MADlib.one_way_anova (label,stdrfss_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'iqrrfss_recev' as columnname,
       (MADlib.one_way_anova (label,iqrrfss_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'maxrfss_recev' as columnname,
       (MADlib.one_way_anova (label,maxrfss_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'minrfss_recev' as columnname,
       (MADlib.one_way_anova (label,minrfss_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'rangerfss_recev' as columnname,
       (MADlib.one_way_anova (label,rangerfss_recev)).*
FROM network_traffic_behaviour_stats_standardized
union all
SELECT 'medrfss_recev' as columnname,
       (MADlib.one_way_anova (label,medrfss_recev)).*
FROM network_traffic_behaviour_stats_standardized
) a
order by p_value
