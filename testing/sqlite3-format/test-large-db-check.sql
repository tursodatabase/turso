SELECT COUNT(*) as total_rows FROM large_db;

SELECT id, text_data FROM large_db WHERE id > 9223372036854775800 ORDER BY id;

SELECT page_count * 1024 as db_size_bytes FROM pragma_page_count();

SELECT COUNT(*) FROM large_aux1;
SELECT COUNT(*) FROM large_aux2;
SELECT COUNT(*) FROM large_overflow;
