drop table if exists cc_svm2,cc_svm2_summary,cc_svm2_random;
select setseed(.65);
SELECT madlib.svm_classification(
    'out_train',
    'CC_SVM2',
    'label',
    'ARRAY[avgpkts_sent,
		stdpkts_sent,
		avgbytes_sent,
		stdbytes_sent,
		avgdur_sent,
		stddur_sent,
		avgbps_sent,
		stdbps_sent,
		count_distinct_dst_ports_sent,
		count_distinct_source_ports_sent,
		count_of_flows_sent,
		iqrbytes_sent,
		iqrpkts_sent,
		iqrdur_sent,
		iqrbps_sent,
		minpkts_sent,
		maxpkts_sent,
		minbytes_sent,
		maxbytes_sent,
		mindur_sent,
		maxdur_sent,
		minbps_sent,
		maxbps_sent,
		rangepkts_sent,
		rangebytes_sent,
		rangedur_sent,
		rangebps_sent,
		medbytes_sent,
		medpkts_sent,
		meddur_sent,
		medbps_sent,
		avgpkts_recev,
		stdpkts_recev,
		avgbytes_recev,
		stdbytes_recev,
		avgdur_recev,
		stddur_recev,
		avgbps_recev,
		stdbps_recev,
		count_distinct_dst_ports_recev,
		count_distinct_source_ports_recev,
		count_of_flows_recev,
		iqrbytes_recev,
		iqrpkts_recev,
		iqrdur_recev,
		iqrbps_recev,
		minpkts_recev,
		maxpkts_recev,
		minbytes_recev,
		maxbytes_recev,
		mindur_recev,
		maxdur_recev,
		minbps_recev,
		maxbps_recev,
		rangepkts_recev,
		rangebytes_recev,
		rangedur_recev,
		rangebps_recev,
		medbytes_recev,
		medpkts_recev,
		meddur_recev,
		medbps_recev,
		frequent_bytes_sent,
		frequent_bytes_recev,
		frequent_packets_sent,
		frequent_packets_recev,
		frequent_duration_sent,
		frequent_duration_recev,
		frequent_bps_sent,
		frequent_bps_recev,
		frequency_of_frequent_bytes_sent,
		frequency_of_frequent_bytes_recev,
		frequency_of_frequent_packets_sent,
		frequency_of_frequent_packets_recev,
		frequency_of_frequent_duration_sent,
		frequency_of_frequent_duration_recev,
		frequency_of_frequent_bps_sent,
		frequency_of_frequent_bps_recev,
		frequent_communucation_frequency_sent,
		frequent_communucation_frequency_recev,
		frequency_of_frequent_communication_frequency_sent,
		frequency_of_frequent_communication_frequency_recev,
		madbytes_sent,
		madbytes_recev,
		madpkts_sent,
		madpkts_recev,
		maddur_sent,
		maddur_recev,
		madbps_sent,
		madbps_recev,
		avgrfss_sent,  
		stdrfss_sent,
		iqrrfss_sent,
		maxrfss_sent,
		rangerfss_sent,
		medrfss_sent,
		avgrfss_recev,  
		stdrfss_recev,
		iqrrfss_recev,
		maxrfss_recev,
		rangerfss_recev,
		medrfss_recev,
		in_degree,
		out_degree,
		in_out_ratio,
                pct_dst_ports_flows_per_ip_sent,
                pct_source_ports_flows_per_ip_recev,
                pct_dst_ports_flows_per_ip_recev
          ]',
    '',  
    'random_state=1500,fit_intercept=True',    
    NULL,
    'norm=L1,lambda=100,class_weight=balanced,max_iter=10000'
    );

--
-- Predict validation set using SVM model built in previous step
--

DROP TABLE IF EXISTS CC_pred2;
SELECT madlib.svm_predict('CC_SVM2', 'out_test', 'id', 'CC_pred2');

--
-- create table with actual label and prediction
--

drop table if exists test_set;
create table test_set as 
select a.id,a.label,b.prediction
from out_test a,CC_pred2 b
where a.id=b.id;


--
-- compute Area under curve value
--

drop table if exists table_out;
SELECT madlib.area_under_roc( 'test_set', 'table_out', 'prediction', 'label');
select * from table_out;

--
-- Build Confusion Matrix
--

DROP TABLE IF EXISTS table_out_cm;
SELECT madlib.confusion_matrix( 'test_set', 'table_out_cm', 'prediction', 'label');
SELECT * FROM table_out_cm ORDER BY class;

--
-- Build tpr,fpr 
--

DROP TABLE IF EXISTS table_out_metrics;
SELECT madlib.binary_classifier( 'test_set', 'table_out_metrics', 'prediction', 'label');
SELECT threshold, tpr, fpr,tp,fp,fn,tn FROM table_out_metrics ORDER BY threshold;
select * from table_out_metrics;






EOF
