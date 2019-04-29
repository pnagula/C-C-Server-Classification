dbname="csimda"
username="gpadmin"
psql $dbname $username << EOF
drop table if exists cc_svm1,cc_svm1_summary,cc_svm1_random;
select setseed(.65);
SELECT madlib.svm_classification(
    'out_train',
    'CC_SVM1',
    'label',
    'ARRAY[ 1,
  count_distinct_dst_ports_sent
,count_distinct_source_ports_recev
,count_of_flows_recev
,count_of_flows_sent
,frequency_of_frequent_bps_recev
,frequency_of_frequent_bps_sent
,frequency_of_frequent_bytes_recev
,frequency_of_frequent_bytes_sent
,frequency_of_frequent_duration_sent
,frequency_of_frequent_packets_recev
,frequency_of_frequent_packets_sent
,frequent_bps_recev
,iqrbps_recev
,iqrbytes_sent
,iqrrfss_recev
,iqrrfss_sent
,madbps_sent
,madbytes_recev
,maddur_recev
,maxbps_recev
,maxdur_sent
,medbps_recev
,medbps_sent
,medbytes_recev
,medbytes_sent
,medrfss_recev
,medrfss_sent
,pct_source_ports_flows_per_ip_recev
,rangebps_recev
,rangebytes_recev
,rangedur_sent
,rangerfss_sent
,stdbytes_recev
,count_distinct_dst_ports_recev
,stdbps_recev
,rangepkts_recev
,maxpkts_recev   
      ]',
    '',  
    'random_state=1500',    
    NULL,
    'init_stepsize=1,class_weight=balanced,max_iter=10000'
    );

--
-- Predict validation set using SVM model built in previous step
--

DROP TABLE IF EXISTS CC_pred2;
SELECT madlib.svm_predict('CC_SVM1', 'out_test', 'id', 'CC_pred2');

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
