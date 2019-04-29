

--
-- PL/Python function to train a GBM classifier, returns model as byte array
--

DROP FUNCTION IF EXISTS gbmclassifier() cascade;
CREATE OR REPLACE FUNCTION gbmclassifier()
RETURNS bytea
AS
$$
	from sklearn.ensemble import GradientBoostingClassifier 
	import numpy as np
	import pickle as pikl
	import pandas as pd

	#train rows are pulled into python result object, order by is important

	x_mat=plpy.execute("select avgpkts_sent,stdpkts_sent,avgbytes_sent,stdbytes_sent,avgdur_sent,stddur_sent,avgbps_sent,stdbps_sent,count_distinct_dst_ports_sent,count_distinct_source_ports_sent,count_of_flows_sent,iqrbytes_sent,iqrpkts_sent,iqrdur_sent,iqrbps_sent,		\
				   minpkts_sent,maxpkts_sent,minbytes_sent,maxbytes_sent,mindur_sent,maxdur_sent,minbps_sent,maxbps_sent,rangepkts_sent,rangebytes_sent,rangedur_sent,rangebps_sent,medbytes_sent,medpkts_sent,meddur_sent,medbps_sent,avgpkts_recev,stdpkts_recev,	\
				   avgbytes_recev,stdbytes_recev,avgdur_recev,stddur_recev,avgbps_recev,stdbps_recev,count_distinct_dst_ports_recev,count_distinct_source_ports_recev,count_of_flows_recev,iqrbytes_recev,iqrpkts_recev,iqrdur_recev,iqrbps_recev,minpkts_recev,		\
				   maxpkts_recev,minbytes_recev,maxbytes_recev,mindur_recev,maxdur_recev,minbps_recev,maxbps_recev,rangepkts_recev,rangebytes_recev,rangedur_recev,rangebps_recev,medbytes_recev,medpkts_recev,meddur_recev,medbps_recev,frequent_bytes_sent,		\
				   frequent_bytes_recev,frequent_packets_sent,frequent_packets_recev,frequent_duration_sent,frequent_duration_recev,frequent_bps_sent,frequent_bps_recev,frequency_of_frequent_bytes_sent,frequency_of_frequent_bytes_recev,frequency_of_frequent_packets_sent, \
				   frequency_of_frequent_packets_recev,frequency_of_frequent_duration_sent,frequency_of_frequent_duration_recev,frequency_of_frequent_bps_sent,frequency_of_frequent_bps_recev,frequent_communucation_frequency_sent,frequent_communucation_frequency_recev,	\
				   frequency_of_frequent_communication_frequency_sent,frequency_of_frequent_communication_frequency_recev,madbytes_sent,madbytes_recev,madpkts_sent,madpkts_recev,maddur_sent,maddur_recev,madbps_sent,madbps_recev,avgrfss_sent,stdrfss_sent,iqrrfss_sent,	\
				   maxrfss_sent,rangerfss_sent,medrfss_sent,avgrfss_recev,stdrfss_recev,iqrrfss_recev,maxrfss_recev,rangerfss_recev,medrfss_recev from out_train order by id;" )
	#labels are pulled - order by is important
	labels=plpy.execute("select label from out_train order by id;")
	plpy.info("length of y is %s"%(len(labels)))
	
	x_mat_d=pd.DataFrame(x_mat[:])
	yl=pd.DataFrame(labels[:])
	
	plpy.info("Going to build GBM model")
	mdl = GradientBoostingClassifier(random_state=10,learning_rate=.01,max_depth=8,max_features = 'sqrt',n_estimators=200)
	plpy.info("fitting GBM model")
	mdl.fit(x_mat_d, yl)
	
	plpy.info("returning now with GBM model byte array")
	return pikl.dumps(mdl,-1)
$$ LANGUAGE PLPYTHONU;

--
-- Call GBM classifier to train the model 
--

drop table if exists gbm_model_bag1;
create table gbm_model_bag1 as 
select gbmclassifier();


--
-- PL/Python function to score using GBM model, parameters are GBM model built and a test row in the form string to overcome 100 parameters limitation of GPDB 
--

DROP FUNCTION IF EXISTS gbm_score(text,bytea) cascade;
CREATE OR REPLACE FUNCTION gbm_score(
		featvector text,
model bytea
)
RETURNS integer
AS
$$
	#plpy.info("inside PL/Python")
	import numpy as np
	import pickle as pikl
	import pandas as pd

	# make up numpy array from column packed as string by using np.fromstring function 

	x_mat=np.fromstring(featvector, dtype=float, sep=',').reshape(1,-1)

	#plpy.info("loading model")
	mdl = pikl.loads(model)
	
	#plpy.info("predicting ...")
	y_hat = mdl.predict(x_mat)
	
	#plpy.info("returning now with prediction for test row")
	return int(y_hat)
$$ LANGUAGE PLPYTHONU;

--
-- create a table with columns packed into a string.
--

drop table if exists featinput;
create table featinput as 
select 	id,label,	avgpkts_sent::text||','||
		stdpkts_sent::text||','||
		avgbytes_sent::text||','||
		stdbytes_sent::text||','||
		avgdur_sent::text||','||
		stddur_sent::text||','||
		avgbps_sent::text||','||
		stdbps_sent::text||','||
		count_distinct_dst_ports_sent::text||','||
		count_distinct_source_ports_sent::text||','||
		count_of_flows_sent::text||','||
		iqrbytes_sent::text||','||
		iqrpkts_sent::text||','||
		iqrdur_sent::text||','||
		iqrbps_sent::text||','||
		minpkts_sent::text||','||
		maxpkts_sent::text||','||
		minbytes_sent::text||','||
		maxbytes_sent::text||','||
		mindur_sent::text||','||
		maxdur_sent::text||','||
		minbps_sent::text||','||
		maxbps_sent::text||','||
		rangepkts_sent::text||','||
		rangebytes_sent::text||','||
		rangedur_sent::text||','||
		rangebps_sent::text||','||
		medbytes_sent::text||','||
		medpkts_sent::text||','||
		meddur_sent::text||','||
		medbps_sent::text||','||
		avgpkts_recev::text||','||
		stdpkts_recev::text||','||
		avgbytes_recev::text||','||
		stdbytes_recev::text||','||
		avgdur_recev::text||','||
		stddur_recev::text||','||
		avgbps_recev::text||','||
		stdbps_recev::text||','||
		count_distinct_dst_ports_recev::text||','||
		count_distinct_source_ports_recev::text||','||
		count_of_flows_recev::text||','||
		iqrbytes_recev::text||','||
		iqrpkts_recev::text||','||
		iqrdur_recev::text||','||
		iqrbps_recev::text||','||
		minpkts_recev::text||','||
		maxpkts_recev::text||','||
		minbytes_recev::text||','||
		maxbytes_recev::text||','||
		mindur_recev::text||','||
		maxdur_recev::text||','||
		minbps_recev::text||','||
		maxbps_recev::text||','||
		rangepkts_recev::text||','||
		rangebytes_recev::text||','||
		rangedur_recev::text||','||
		rangebps_recev::text||','||
		medbytes_recev::text||','||
		medpkts_recev::text||','||
		meddur_recev::text||','||
		medbps_recev::text||','||
		frequent_bytes_sent::text||','||
		frequent_bytes_recev::text||','||
		frequent_packets_sent::text||','||
		frequent_packets_recev::text||','||
		frequent_duration_sent::text||','||
		frequent_duration_recev::text||','||
		frequent_bps_sent::text||','||
		frequent_bps_recev::text||','||
		frequency_of_frequent_bytes_sent::text||','||
		frequency_of_frequent_bytes_recev::text||','||
		frequency_of_frequent_packets_sent::text||','||
		frequency_of_frequent_packets_recev::text||','||
		frequency_of_frequent_duration_sent::text||','||
		frequency_of_frequent_duration_recev::text||','||
		frequency_of_frequent_bps_sent::text||','||
		frequency_of_frequent_bps_recev::text||','||
		frequent_communucation_frequency_sent::text||','||
		frequent_communucation_frequency_recev::text||','||
		frequency_of_frequent_communication_frequency_sent::text||','||
		frequency_of_frequent_communication_frequency_recev::text||','||
		madbytes_sent::text||','||
		madbytes_recev::text||','||
		madpkts_sent::text||','||
		madpkts_recev::text||','||
		maddur_sent::text||','||
		maddur_recev::text||','||
		madbps_sent::text||','||
		madbps_recev::text||','||
		avgrfss_sent::text||','||  
		stdrfss_sent::text||','||
		iqrrfss_sent::text||','||
		maxrfss_sent::text||','||
		rangerfss_sent::text||','||
		medrfss_sent::text||','||
		avgrfss_recev::text||','||  
		stdrfss_recev::text||','||
		iqrrfss_recev::text||','||
		maxrfss_recev::text||','||
		rangerfss_recev::text||','||
		medrfss_recev::text as featvector
 from out_test;

--
-- call GBM scoring PL/Python function using test set and score one row at a time
--

drop table if exists output_bag1;
create table output_bag1 as 
select id,label,gbm_score(featvector,gbmclassifier) 
from gbm_model_bag1,featinput 
distributed randomly;
