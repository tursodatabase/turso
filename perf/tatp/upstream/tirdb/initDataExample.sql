--
-- TATP benchmark
--      This is an example file to show the initial information required in 
--      TIRDB before a TATP benchmark run (replace the data below, or add rows,  
--      to corespond ro your environment)

INSERT INTO _databases        (name, version, release_date, supplier) 
VALUES                        ('solidDB','6.3.0015','2008-12-09','IBM');
INSERT INTO _databases VALUES ('solidDB','6.3.0027','2008-12-09','IBM');
INSERT INTO _databases VALUES ('Oracle','10.1.0.3','2005-05-04','Oracle');
INSERT INTO _databases VALUES ('MySQL/InnoDB','5.0','2006-05-31','Sun');

INSERT INTO operating_systems        (name, version, supplier)
VALUES                               ('Linux','Fedora Core 5','Red Hat');
INSERT INTO operating_systems VALUES ('Linux','SLES10','Novell');
INSERT INTO operating_systems VALUES ('MS Windows','XP','Microsoft');
INSERT INTO operating_systems VALUES ('AIX','5.3','IBM');

INSERT INTO hardware
(hardware_id, processor_id, num_of_processors, bios_version, bus_id, memory_id, memory_amount_gb, swap_file_size_mb, disk_id, num_of_disks, comments) 
VALUES 
('sut1_x86','xeon1','2','2/25/2','bus1','pc800_ecc_rdram', 2000,'2','disk1','2','Typical low-end 2CPU PC');
INSERT INTO hardware VALUES
('sut2_Xeon5570', 'Xeon_5570', 2,'2/25/2',NULL,	'pc800_ecc_rdram', 2000, NULL, 'disk-ssd', 3, 'Intel Nehalem Xeon 5570, 8 cores');
INSERT INTO hardware VALUES 
('Thinkpad-61p', 'T7700', 1, NULL ,NULL, 'DDR2 SDRAM', 2, NULL, 'Serial ATA-150', 1, 'Intel Core 2 Duo T7700, 2 cores');
INSERT INTO hardware VALUES 
('x86', 'Xeon', 1, NULL ,NULL, 'DDR2 SDRAM ', 2, NULL, 'Serial ATA', 1, 'Intel');
INSERT INTO hardware VALUES 
('P6-595','POWER6','8',NULL, NULL,'DDR2', 2000, 1000,'disk-hdd','2','Fully loaded P6 595 with 64 cores');

-- Populating these tables is optional:

INSERT INTO processor_types
(id, speed_ghz, l1_memory, l2_memory, l3_memory, num_of_cores, num_of_threads, supplier)
VALUES ('xeon1',1.97,'512B','32K','1MB', 1, 2, 'Intel');
INSERT INTO processor_types 
VALUES ('Xeon_5570',  2.93, '64K', '256K', '8MB', 4, 8, 'Intel');
INSERT INTO processor_types 
VALUES ('T7700', 2.4, NULL, '4M', NULL, 2, 2, 'Intel');
INSERT INTO processor_types 
VALUES ('POWER6', 4.2, '128K','4M','32M',8, 2, 'IBM');

INSERT INTO memory_types        (id, access_time_ns, _type, supplier)
VALUES                          ('pc800_ecc_rdram',NULL,NULL,NULL);
INSERT INTO memory_types VALUES ('DDR2 SDRAM',NULL,NULL,NULL);

INSERT INTO disk_types        (id, _type, access_time_ms, transfer_speed_mbps, supplier)
VALUES                        ('disk1','scsi',NULL,NULL,NULL);
INSERT INTO disk_types VALUES ('Serial ATA','HDD ATA',NULL,NULL,NULL);
INSERT INTO disk_types VALUES ('disk-ssd','SSD SCSI',NULL,NULL,NULL);
INSERT INTO disk_types VALUES ('disk-hdd','HDD SCSI',NULL,NULL,NULL);

INSERT INTO bus_types (id, speed_mbps, width, supplier)
VALUES                ('bus1','400',NULL,NULL);


