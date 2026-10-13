-- TATP
-- TIRDB schema definition file

-- Remove potentially conflicting tables first

DROP TABLE /*! if exists */ transaction_mixes;
DROP TABLE /*! if exists */ database_client_distributions;
DROP TABLE /*! if exists */ result_response_scale;
DROP TABLE /*! if exists */ result_response_90percentile;
DROP TABLE /*! if exists */ result_throughput;
DROP TABLE /*! if exists */ monitoring_data;
DROP TABLE /*! if exists */ test_runs;
DROP TABLE /*! if exists */ test_sessions;
DROP TABLE /*! if exists */ config_data;
DROP TABLE /*! if exists */ hardware;
DROP TABLE /*! if exists */ operating_systems;
DROP TABLE /*! if exists */ _databases;
DROP TABLE /*! if exists */ disk_types;
DROP TABLE /*! if exists */ bus_types;
DROP TABLE /*! if exists */ processor_types;
DROP TABLE /*! if exists */ memory_types;


CREATE TABLE memory_types (
	id CHAR(32) NOT NULL,
	access_time_ns FLOAT,
	_type CHAR(32),
	supplier CHAR(32),
	PRIMARY KEY (id));

-- Create new table PROCESSOR_TYPES.
CREATE TABLE processor_types (
	id CHAR(32) NOT NULL,
	speed_ghz FLOAT,
	l1_memory CHAR(32),
	l2_memory CHAR(32),
	l3_memory CHAR(32),
	num_of_cores INTEGER,
	num_of_threads INTEGER,
	supplier CHAR(32),
	PRIMARY KEY (id));

-- Create new table BUS_TYPES.
CREATE TABLE bus_types (
	id CHAR(32) NOT NULL,
	speed_mbps INTEGER,
	width INTEGER,
	supplier CHAR(32),
	PRIMARY KEY (id));

-- Create new table DISK_TYPES.
CREATE TABLE disk_types (
	id CHAR(32) NOT NULL,
	_type CHAR(32),
	access_time_ms FLOAT,
	transfer_speed_mbps INTEGER,
	supplier CHAR(32),
	PRIMARY KEY (id));

-- Create new table _DATABASES.
CREATE TABLE _databases (
	name CHAR(32) NOT NULL,
	version CHAR(32) NOT NULL,
	release_date DATE,
	supplier CHAR(32),
	PRIMARY KEY (name, version));

-- Create new table OPERATING_SYSTEMS.
CREATE TABLE operating_systems (
	name CHAR(32) NOT NULL,
	version CHAR(32) NOT NULL,
	supplier CHAR(32),
	PRIMARY KEY (name, version));

-- Create new table HARDWARE.
CREATE TABLE hardware (
	hardware_id CHAR(32) NOT NULL,
	processor_id CHAR(32),
	num_of_processors INTEGER,
	bios_version CHAR(32),
	bus_id CHAR(32),
	memory_id CHAR(32),
	memory_amount_gb INTEGER,
	swap_file_size_mb INTEGER,
	disk_id CHAR(32),
	num_of_disks INTEGER,
	comments CHAR(255),
	PRIMARY KEY (hardware_id));

-- Create new table CONFIG_DATA.
CREATE TABLE config_data (
	config_id CHAR(128) NOT NULL,
	config_name CHAR(128) NOT NULL,
	config_file VARCHAR(32000),
	config_comments VARCHAR(4096),
	PRIMARY KEY (config_id, config_name));


-- Create new table TEST_SESSIONS.
CREATE TABLE test_sessions (
	session_id INTEGER NOT NULL,
	session_name CHAR(255),
	start_date DATE,
	start_time TIME,
	stop_date DATE,
	stop_time TIME,
	author CHAR(32),
	db_name CHAR(32),
	db_version CHAR(32),
	hardware_id CHAR(32),
	os_name CHAR(32),
	os_version CHAR(32),
	throughput_resolution INTEGER,
	config_id CHAR(128),
	config_name CHAR(128),
	software_version CHAR(32),
	comments CHAR(255),
	PRIMARY KEY (session_id),
	CONSTRAINT fk_ts_db FOREIGN KEY (db_name,db_version) 
		REFERENCES _databases(name, version),
	CONSTRAINT fk_ts_hardware FOREIGN KEY (hardware_id) 
		REFERENCES hardware(hardware_id),
	CONSTRAINT fk_ts_os FOREIGN KEY (os_name, os_version) 
		REFERENCES operating_systems(name, version),
	CONSTRAINT fk_ts_config FOREIGN KEY (config_id, config_name) 
		REFERENCES config_data(config_id, config_name));

-- Create new table TEST_RUNS
CREATE TABLE test_runs (
	test_run_id INTEGER NOT NULL,
	session_id INTEGER,
	test_name CHAR(255),
	start_date DATE,
	start_time TIME,
	stop_date DATE,
	stop_time TIME,
	test_completed INTEGER,
	test_number INTEGER,
	client_count INTEGER,
	rampup_time INTEGER,
	subscribers INTEGER,
	mqth_avg INTEGER,
	PRIMARY KEY (test_run_id),
	CONSTRAINT fk_trt_session FOREIGN KEY (session_id) 
		REFERENCES test_sessions(session_id)
		ON UPDATE CASCADE
		ON DELETE CASCADE);

-- Create new table MONITORING_DATA
CREATE TABLE monitoring_data (
	test_run_id INTEGER NOT NULL,
	mon_type CHAR(128) NOT NULL,
	mon_data VARCHAR(60000),
	PRIMARY KEY (test_run_id, mon_type),
	CONSTRAINT fk_mdt_test_run_id FOREIGN KEY (test_run_id) 
		REFERENCES test_runs(test_run_id)
		ON UPDATE CASCADE
		ON DELETE CASCADE);

-- Create new table RESULT_THROUGHPUT.
CREATE TABLE result_throughput (
	test_run_id INTEGER NOT NULL,
	time_slot_num INTEGER NOT NULL,
	mqth INTEGER,
	PRIMARY KEY (test_run_id, time_slot_num),
	CONSTRAINT fk_rt_test_run FOREIGN KEY (test_run_id) 
		REFERENCES test_runs(test_run_id)
		ON UPDATE CASCADE
		ON DELETE CASCADE);


-- Create new table RESULT_RESPONSE_SCALE.
CREATE TABLE result_response_scale (
	test_run_id INTEGER NOT NULL,
	transaction_type CHAR(32) NOT NULL,
	slot INTEGER NOT NULL,
	bound INTEGER NOT NULL,
	num_of_hits INTEGER,
	PRIMARY KEY (test_run_id, transaction_type, slot),
	CONSTRAINT fk_rrs_test_run FOREIGN KEY (test_run_id) 
		REFERENCES test_runs(test_run_id)
		ON UPDATE CASCADE
		ON DELETE CASCADE);

-- Create new table RESULT_RESPONSE_90PERCENTILE.
CREATE TABLE result_response_90percentile (
	test_run_id INTEGER NOT NULL,
	transaction_type CHAR(32) NOT NULL,
	resp_time INTEGER,
	PRIMARY KEY (test_run_id, transaction_type),
	CONSTRAINT fk_rrs90 FOREIGN KEY (test_run_id) 
		REFERENCES test_runs(test_run_id)
		ON UPDATE CASCADE
		ON DELETE CASCADE);

-- Create new table TRANSACTION_MIXES.
CREATE TABLE transaction_mixes (
	test_run_id INTEGER NOT NULL,
	transaction_type CHAR(255) NOT NULL,
	percentage TINYINT,
	PRIMARY KEY (test_run_id, transaction_type),
	CONSTRAINT fk_tm_test_run FOREIGN KEY (test_run_id) 
		REFERENCES test_runs(test_run_id)
		ON UPDATE CASCADE
		ON DELETE CASCADE);

-- Create new table DATABASE_CLIENT_DISTRIBUTIONS.
CREATE TABLE database_client_distributions (
	test_run_id INTEGER NOT NULL,
	remote_name CHAR(255) NOT NULL,
	remote_ip CHAR(64) NOT NULL,
	client_count INTEGER NOT NULL,
	PRIMARY KEY (test_run_id, remote_name),
	CONSTRAINT fk_dcd_test_run FOREIGN KEY (test_run_id)
		REFERENCES test_runs(test_run_id)
		ON UPDATE CASCADE
		ON DELETE CASCADE);
