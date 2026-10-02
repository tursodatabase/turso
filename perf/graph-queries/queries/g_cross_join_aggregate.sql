SELECT AVG(d.total) AS avgPerDay, COUNT(*) AS days
FROM (
  SELECT date(createdAt / 1000, 'unixepoch') AS day, COUNT(*) AS total
  FROM message
  WHERE author = 'student'
  GROUP BY day
) AS d
CROSS JOIN (
  SELECT MIN(createdAt) AS firstCreated
  FROM message
  WHERE author = 'student'
) AS f
WHERE d.day > date(f.firstCreated / 1000, 'unixepoch')
