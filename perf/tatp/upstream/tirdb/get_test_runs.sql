-- TATP
-- Retrieve all test runs satisfying the conditions
select 
cast(se.SESSION_ID as char(6)) as SES_ID, 
cast(SUBSTRING (SESSION_NAME, 1, 24) as char(24)) as SESSION,
cast(test_run_id as char(6)) as RUN_ID,
cast(SUBSTRING (TEST_NAME, 1, 18) as char(18)) as TEST_NAME,
-- cast(SUBSTRING (OS_NAME, 1, 5) as char(5)) as OS,
-- cast(SUBSTRING (HARDWARE_ID, 1, 16) as char(16)) as HARDWARE_ID,
te.START_DATE, 
te.START_TIME, te.STOP_TIME,
-- cast(SUBSTRING (DB_NAME,1, 8) as char(8)) as PRODUCT,
cast(SUBSTRING (DB_VERSION,1, 12) as char(12)) as VERSION, 
-- cast(SUBSTRING (config_id,1, 10) as char(10)) as CONFIG_ID,
cast(SUBSTRING (CONFIG_NAME,1, 36) as char(36)) as CONFIG, 
mqth_avg,
CLIENT_COUNT as CLIENTS,
SUBSCRIBERS as SUBSCR
from Test_sessions se join test_runs te on se.SESSION_ID = te.SESSION_ID
where 
se.start_date > '2009-01-01'
-- and HARDWARE_ID = 'sut2_Xeon5570'
-- and session_name like '%RIS%'
-- and db_version ='6.1.0014'
-- and config_name like 'acc%'
order by test_run_id DESC;

