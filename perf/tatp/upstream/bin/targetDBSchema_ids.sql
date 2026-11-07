//tatp_sql
//
// This is an example target database schema file of the TATP Benchmark Suite.
// All the SQL commands defined in this file are executed against the target 
// database when encountering a 'population' directive (mode 'populate') in a TDF file. 
// The commands are processed before the actual population process starts. 
// The commands are processed in the order they appear in this file.

// The valid TATP schema definition follows 

DROP TABLE  call_forwarding;
DROP TABLE  special_facility;
DROP TABLE  access_info;
DROP TABLE  subscriber;
DROP TABLE  tps;

CREATE TABLE subscriber 
	(s_id INTEGER NOT NULL PRIMARY KEY, sub_nbr VARCHAR(15) NOT NULL UNIQUE, 
	bit_1 SMALLINT, bit_2 SMALLINT, bit_3 SMALLINT, bit_4 SMALLINT, bit_5 SMALLINT, 
	bit_6 SMALLINT, bit_7 SMALLINT, bit_8 SMALLINT, bit_9 SMALLINT, bit_10 SMALLINT, 
	hex_1 SMALLINT, hex_2 SMALLINT, hex_3 SMALLINT, hex_4 SMALLINT, hex_5 SMALLINT, 
	hex_6 SMALLINT, hex_7 SMALLINT, hex_8 SMALLINT, hex_9 SMALLINT, hex_10 SMALLINT, 
	byte2_1 SMALLINT, byte2_2 SMALLINT, byte2_3 SMALLINT, byte2_4 SMALLINT, byte2_5 SMALLINT, 
	byte2_6 SMALLINT, byte2_7 SMALLINT, byte2_8 SMALLINT, byte2_9 SMALLINT, byte2_10 SMALLINT, 
	msc_location INTEGER, vlr_location INTEGER) lock mode row;

CREATE TABLE access_info 
	(s_id INTEGER NOT NULL, ai_type SMALLINT NOT NULL, 
	data1 SMALLINT, data2 SMALLINT, data3 CHAR(3), data4 CHAR(5), 
	PRIMARY KEY (s_id, ai_type), 
	FOREIGN KEY (s_id) REFERENCES subscriber (s_id));

CREATE TABLE special_facility 
	(s_id INTEGER NOT NULL, sf_type SMALLINT NOT NULL, is_active SMALLINT NOT NULL, 
	error_cntrl SMALLINT, data_a SMALLINT, data_b CHAR(5), 
	PRIMARY KEY (s_id, sf_type), 
	FOREIGN KEY (s_id) REFERENCES subscriber (s_id)) lock mode row;

CREATE TABLE call_forwarding 
	(s_id INTEGER NOT NULL, sf_type SMALLINT NOT NULL, start_time SMALLINT NOT NULL, 
	end_time SMALLINT, numberx VARCHAR(15), 
	PRIMARY KEY (s_id, sf_type, start_time), 
	FOREIGN KEY (s_id, sf_type) REFERENCES special_facility(s_id, sf_type))
  lock mode row;

// table for real-time collection of performance numbers
// one row per client
CREATE TABLE tps
        (id INTEGER NOT NULL PRIMARY KEY, value INTEGER, db1 INTEGER, db2 INTEGER) lock mode row;

