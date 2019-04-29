
--
-- Predict validation set using SVM model built in previous step
--

DROP TABLE IF EXISTS CC_pred;
SELECT madlib.svm_predict('CC_SVM', 'out_test', 'id', 'CC_pred');

--
-- create table with actual label and prediction
--

drop table if exists test_set;
create table test_set as 
select a.id,a.label,b.prediction
from out_test a,CC_pred b
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

