SELECT last_value, is_called FROM public.unused_sequence;
SELECT last_value, is_called FROM public.used_sequence;
SELECT last_value, is_called FROM public.descending_sequence;
SELECT last_value, is_called FROM public.reset_sequence;
SELECT last_value, is_called FROM public.large_sequence;
SELECT last_value, is_called FROM public.cycling_sequence;
SELECT nextval('public.large_sequence'), nextval('public.cycling_sequence');
SELECT nextval('public.unused_sequence'), nextval('public.used_sequence'), nextval('public.descending_sequence'), nextval('public.reset_sequence');
SELECT c.relname, s.seqtypid, s.seqstart, s.seqincrement, s.seqmax, s.seqmin, s.seqcache, s.seqcycle
FROM pg_sequence s JOIN pg_class c ON c.oid = s.seqrelid ORDER BY c.relname;
