//tatp_sql
//
// This is an example target database schema file of the TATP Benchmark Suite.
// All the SQL commands defined in this file are executed against the target 
// database when encountering a 'population' directive (mode 'populate') in a TDF file. 
// The commands are processed before the actual population process starts. 
// The commands are processed in the order they appear in this file.

// The valid TATP schema definition follows 

DROP TABLE call_forwarding;
DROP TABLE special_facility;
DROP TABLE access_info;
DROP TABLE subscriber;
DROP TABLE tps;

CREATE TABLE subscriber 
	(s_id INTEGER NOT NULL PRIMARY KEY, sub_nbr VARCHAR(15) NOT NULL UNIQUE, 
	bit_1 TINYINT, bit_2 TINYINT, bit_3 TINYINT, bit_4 TINYINT, bit_5 TINYINT, 
	bit_6 TINYINT, bit_7 TINYINT, bit_8 TINYINT, bit_9 TINYINT, bit_10 TINYINT, 
	hex_1 TINYINT, hex_2 TINYINT, hex_3 TINYINT, hex_4 TINYINT, hex_5 TINYINT, 
	hex_6 TINYINT, hex_7 TINYINT, hex_8 TINYINT, hex_9 TINYINT, hex_10 TINYINT, 
	byte2_1 SMALLINT, byte2_2 SMALLINT, byte2_3 SMALLINT, byte2_4 SMALLINT, byte2_5 SMALLINT, 
	byte2_6 SMALLINT, byte2_7 SMALLINT, byte2_8 SMALLINT, byte2_9 SMALLINT, byte2_10 SMALLINT, 
	msc_location INTEGER, vlr_location INTEGER);

CREATE TABLE access_info 
	(s_id INTEGER NOT NULL, ai_type TINYINT NOT NULL, 
	data1 SMALLINT, data2 SMALLINT, data3 CHAR(3), data4 CHAR(5), 
	PRIMARY KEY (s_id, ai_type), 
	FOREIGN KEY (s_id) REFERENCES subscriber (s_id));

CREATE TABLE special_facility 
	(s_id INTEGER NOT NULL, sf_type TINYINT NOT NULL, is_active TINYINT NOT NULL, 
	error_cntrl SMALLINT, data_a SMALLINT, data_b CHAR(5), 
	PRIMARY KEY (s_id, sf_type), 
	FOREIGN KEY (s_id) REFERENCES subscriber (s_id));

CREATE TABLE call_forwarding 
	(s_id INTEGER NOT NULL, sf_type TINYINT NOT NULL, start_time TINYINT NOT NULL, 
	end_time TINYINT, numberx VARCHAR(15), 
	PRIMARY KEY (s_id, sf_type, start_time), 
	FOREIGN KEY (s_id, sf_type) REFERENCES special_facility(s_id, sf_type));

CREATE TABLE tps
        (id INTEGER NOT NULL PRIMARY KEY, value INTEGER, db1 INTEGER, db2 INTEGER);