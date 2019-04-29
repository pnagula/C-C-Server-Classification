
--
-- split data train and validation (70-30)
--

DROP TABLE IF EXISTS out_train,out_test;
select setseed(.2);
SELECT madlib.train_test_split(
                                'network_traffic_behaviour_stats_standardized',    -- Source table
                                'out',     -- Output table
                                0.8,       -- train_proportion
                                NULL,      -- Default = 1 - train_proportion = 0.5
                                NULL, -- Strata definition
                                '*', -- Columns to output
                                FALSE,      -- Sample with replacement
                                True); 
