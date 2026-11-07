-- TATP
-- Retrieve the respionde time histogram for a given 
-- trabsaction type and test run
select 
slot, 
bound, 
num_of_hits from result_response_scale
where test_run_id=1 and transaction_type='GET_SUBSCRIBER_DATA' and bound < 10000;


select 
slot, 
bound,num_of_hits from result_response_scale where test_run_id=1 and transaction_type='UPDATE_LOCATION' and bound < 10000;