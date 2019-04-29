--
-- create a user defined type for returning result set
--

drop type if exists balancedset cascade;
create type balancedset as (
  avgpkts_sent double precision,
  stdpkts_sent double precision,
  avgbytes_sent double precision,
  stdbytes_sent double precision,
  avgdur_sent double precision,
  stddur_sent double precision,
  avgbps_sent double precision,
  stdbps_sent double precision,
  count_distinct_dst_ports_sent double precision,
  count_distinct_source_ports_sent double precision,
  count_of_flows_sent double precision,
  iqrbytes_sent double precision,
  iqrpkts_sent double precision,
  iqrdur_sent double precision,
  iqrbps_sent double precision,
  minpkts_sent double precision,
  maxpkts_sent double precision,
  minbytes_sent double precision,
  maxbytes_sent double precision,
  mindur_sent double precision,
  maxdur_sent double precision,
  minbps_sent double precision,
  maxbps_sent double precision,
  rangepkts_sent double precision,
  rangebytes_sent double precision,
  rangedur_sent double precision,
  rangebps_sent double precision,
  medbytes_sent double precision,
  medpkts_sent double precision,
  meddur_sent double precision,
  medbps_sent double precision,
  avgpkts_recev double precision,
  stdpkts_recev double precision,
  avgbytes_recev double precision,
  stdbytes_recev double precision,
  avgdur_recev double precision,
  stddur_recev double precision,
  avgbps_recev double precision,
  stdbps_recev double precision,
  count_distinct_dst_ports_recev double precision,
  count_distinct_source_ports_recev double precision,
  count_of_flows_recev double precision,
  iqrbytes_recev double precision,
  iqrpkts_recev double precision,
  iqrdur_recev double precision,
  iqrbps_recev double precision,
  minpkts_recev double precision,
  maxpkts_recev double precision,
  minbytes_recev double precision,
  maxbytes_recev double precision,
  mindur_recev double precision,
  maxdur_recev double precision,
  minbps_recev double precision,
  maxbps_recev double precision,
  rangepkts_recev double precision,
  rangebytes_recev double precision,
  rangedur_recev double precision,
  rangebps_recev double precision,
  medbytes_recev double precision,
  medpkts_recev double precision,
  meddur_recev double precision,
  medbps_recev double precision,
  frequent_bytes_sent double precision,
  frequent_bytes_recev double precision,
  frequent_packets_sent double precision,
  frequent_packets_recev double precision,
  frequent_duration_sent double precision,
  frequent_duration_recev double precision,
  frequent_bps_sent double precision,
  frequent_bps_recev double precision,
  frequency_of_frequent_bytes_sent double precision,
  frequency_of_frequent_bytes_recev double precision,
  frequency_of_frequent_packets_sent double precision,
  frequency_of_frequent_packets_recev double precision,
  frequency_of_frequent_duration_sent double precision,
  frequency_of_frequent_duration_recev double precision,
  frequency_of_frequent_bps_sent double precision,
  frequency_of_frequent_bps_recev double precision,
  frequent_communucation_frequency_sent double precision,
  frequent_communucation_frequency_recev double precision,
  frequency_of_frequent_communication_frequency_sent double precision,
  frequency_of_frequent_communication_frequency_recev double precision,
  madbytes_sent double precision,
  madbytes_recev double precision,
  madpkts_sent double precision,
  madpkts_recev double precision,
  maddur_sent double precision,
  maddur_recev double precision,
  madbps_sent double precision,
  madbps_recev double precision,
  avgrfss_sent double precision,
  stdrfss_sent double precision,
  iqrrfss_sent double precision,
  maxrfss_sent double precision,
  minrfss_sent double precision,
  rangerfss_sent double precision,
  medrfss_sent double precision,
  avgrfss_recev double precision,
  stdrfss_recev double precision,
  iqrrfss_recev double precision,
  maxrfss_recev double precision,
  minrfss_recev double precision,
  rangerfss_recev double precision,
  medrfss_recev double precision,
  label integer
);

--
-- PL/Python code to SMOTE (Synthetic Minority Over Sampling Technique Estimator)
--

DROP FUNCTION IF EXISTS smote(float[], int, int[],int,int);
CREATE OR REPLACE FUNCTION smote(
features_matrix_linear float[],
num_features int,
labels int[],
r0 int,
r1 int
)
RETURNS  setof balancedset
AS
$$
	from imblearn.over_sampling import SMOTE
	import pandas as pd
	import numpy as np
	y = np.asarray(labels)
	plpy.info("length of y is %s"%(len(y)))
	
	#decomposing array of arrays to a numpy matrix (Rows,Columns) of table 
 
	x_mat = np.array(features_matrix_linear).reshape(len(features_matrix_linear)/num_features, num_features)
	plpy.info("shape of mat is %s x %s" %(x_mat.shape[0], x_mat.shape[1]))
	xt=pd.DataFrame(x_mat[:]) 
	yt=pd.DataFrame(y[:]) 
	
	play.info("Calling SMOTE function...")
	sm = SMOTE(random_state=12,ratio={0:r0,1:r1})
	x_train_res, y_train_res = sm.fit_sample(xt, yt)
	xt=pd.DataFrame(x_train_res[:],columns=[ 'avgpkts_sent',
							'stdpkts_sent',
							'avgbytes_sent',
							'stdbytes_sent',
							'avgdur_sent',
							'stddur_sent',
							'avgbps_sent',
							'stdbps_sent',
							'count_distinct_dst_ports_sent',
							'count_distinct_source_ports_sent',
							'count_of_flows_sent',
							'iqrbytes_sent',
							'iqrpkts_sent',
							'iqrdur_sent',
							'iqrbps_sent',
							'minpkts_sent',
							'maxpkts_sent',
							'minbytes_sent',
							'maxbytes_sent',
							'mindur_sent',
							'maxdur_sent',
							'minbps_sent',
							'maxbps_sent',
							'rangepkts_sent',
							'rangebytes_sent',
							'rangedur_sent',
							'rangebps_sent',
							'medbytes_sent',
							'medpkts_sent',
							'meddur_sent',
							'medbps_sent',
							'avgpkts_recev',
							'stdpkts_recev',
							'avgbytes_recev',
							'stdbytes_recev',
							'avgdur_recev',
							'stddur_recev',
							'avgbps_recev',
							'stdbps_recev',
							'count_distinct_dst_ports_recev',
							'count_distinct_source_ports_recev',
							'count_of_flows_recev',
							'iqrbytes_recev',
							'iqrpkts_recev',
							'iqrdur_recev',
							'iqrbps_recev',
							'minpkts_recev',
							'maxpkts_recev',
							'minbytes_recev',
							'maxbytes_recev',
							'mindur_recev',
							'maxdur_recev',
							'minbps_recev',
							'maxbps_recev',
							'rangepkts_recev',
							'rangebytes_recev',
							'rangedur_recev',
							'rangebps_recev',
							'medbytes_recev',
							'medpkts_recev',
							'meddur_recev',
							'medbps_recev',
							'frequent_bytes_sent',
							'frequent_bytes_recev',
							'frequent_packets_sent',
							'frequent_packets_recev',
							'frequent_duration_sent',
							'frequent_duration_recev',
							'frequent_bps_sent',
							'frequent_bps_recev',
							'frequency_of_frequent_bytes_sent',
							'frequency_of_frequent_bytes_recev',
							'frequency_of_frequent_packets_sent',
							'frequency_of_frequent_packets_recev',
							'frequency_of_frequent_duration_sent',
							'frequency_of_frequent_duration_recev',
							'frequency_of_frequent_bps_sent',
							'frequency_of_frequent_bps_recev',
							'frequent_communucation_frequency_sent',
							'frequent_communucation_frequency_recev',
							'frequency_of_frequent_communication_frequency_sent',
							'frequency_of_frequent_communication_frequency_recev',
							'madbytes_sent',
							'madbytes_recev',
							'madpkts_sent',
							'madpkts_recev',
							'maddur_sent',
							'maddur_recev',
							'madbps_sent',
							'madbps_recev',
							'avgrfss_sent',
							'stdrfss_sent',
							'iqrrfss_sent',
							'maxrfss_sent',
							'minrfss_sent',
							'rangerfss_sent',
							'medrfss_sent',
							'avgrfss_recev',
							'stdrfss_recev',
							'iqrrfss_recev',
							'maxrfss_recev',
							'minrfss_recev',
							'rangerfss_recev',
							'medrfss_recev'])
	yt=pd.DataFrame(y_train_res[:],columns=['label'])
	train_set=pd.concat([xt,yt],axis=1)
	
	#convert python pandas dataframe to list of lists so that GPDB can convert list of lists to GPDB composite type
	
	ts=list(list(x) for x in zip(*(train_set[x].values.tolist() for x in train_set.columns)))
	plpy.info('returning now')
        return(ts)
$$ LANGUAGE PLPYTHONU;

--
-- create table with row sampled and with columns packed as array A.K.A feature vector
--

drop table if exists featurevec;
create table featurevec as 
(select id,label,array[			avgpkts_sent,
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
							minrfss_sent,
							rangerfss_sent,
							medrfss_sent,
							avgrfss_recev,
							stdrfss_recev,
							iqrrfss_recev,
							maxrfss_recev,
							minrfss_recev,
							rangerfss_recev,
							medrfss_recev] as feature_vector
					from out_train
					where label = 0
					limit 16666)
		union
		select id,label,array[			avgpkts_sent,
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
							minrfss_sent,
							rangerfss_sent,
							medrfss_sent,
							avgrfss_recev,
							stdrfss_recev,
							iqrrfss_recev,
							maxrfss_recev,
							minrfss_recev,
							rangerfss_recev,
							medrfss_recev] as feature_vector
					from out_train where label=1 order by id
distributed randomly					
;


--
-- create user defined aggregate function array_agg_array to make up Array of Arrays i.e. {{c1,...c104},{c1,...c104},...n}
--

drop aggregate if exists array_agg_array(anyarray) cascade;
create ordered aggregate array_agg_array(anyarray)
(
SFUNC = array_cat,
STYPE = anyarray
);

--
-- Create table with a single row and column which contains Array of Arrays of Rows -- This is first parameter for SMOTE function
--

drop table if exists featinput1;
create table featinput1 as 
select array_agg_array(feature_vector) from (select feature_vector from featurevec order by id) a
distributed randomly
;

--
-- create table with a single row and column which contain array of labels  -- This is second parameter for SMOTE function
--

drop table if exists featinput2;
create table featinput2 as 
select array_agg(label) from (select label from featurevec order by id) b
distributed randomly
;

--
-- call PL/Python SMOTE function to create balanced dataset
--
					
drop table if exists balanced_trainset;
create table balanced_trainset as 
select
	* from smote((select * from featinput1), 104, (select * from featinput2),16666,5000)
distributed randomly;



