-- TATP
-- Retrieve all 90% reposnse times for a given test run
select transaction_type, resp_time
from result_response_90percentile
where test_run_id = 1;