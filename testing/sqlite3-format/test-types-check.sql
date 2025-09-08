SELECT i, r, t, hex(b) as b, n, CASE WHEN typeof(x) = 'blob' THEN hex(x) ELSE x END as x FROM types_test ORDER BY i;
