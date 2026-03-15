SELECT a.date, a.category, SUM(a.total) AS total
FROM (
  SELECT id, date(datetime(createdAt / 1000, 'unixepoch', '+60 minutes')) AS date,
    'messages' AS category, COUNT(*) AS total
  FROM message
  WHERE createdAt >= 1772582400000 AND createdAt <= 1772668799999
    AND author='student'
  GROUP BY date
  UNION ALL
  SELECT id, date(datetime(startDate / 1000, 'unixepoch', '+60 minutes')) AS date,
    'sessions' AS category, COUNT(*) AS total
  FROM conversation
  WHERE startDate >= 1772582400000 AND startDate <= 1772668799999
  GROUP BY date
  UNION ALL
  SELECT id, date(datetime(createdAt / 1000, 'unixepoch', '+60 minutes')) AS date,
    'assessments' AS category, COUNT(*) AS total
  FROM node
  WHERE createdAt >= 1772582400000 AND createdAt <= 1772668799999
    AND (type='exam' OR type='assignment')
  GROUP BY date
  UNION ALL
  SELECT id, date(datetime(createdAt / 1000, 'unixepoch', '+60 minutes')) AS date,
    'assignments' AS category, COUNT(*) AS total
  FROM node
  WHERE createdAt >= 1772582400000 AND createdAt <= 1772668799999
    AND type='assignment'
  GROUP BY date
  UNION ALL
  SELECT id, date(datetime(createdAt / 1000, 'unixepoch', '+60 minutes')) AS date,
    'exams' AS category, COUNT(*) AS total
  FROM node
  WHERE createdAt >= 1772582400000 AND createdAt <= 1772668799999
    AND type='exam'
  GROUP BY date
  UNION ALL
  SELECT id, date(datetime(createdAt / 1000, 'unixepoch', '+60 minutes')) AS date,
    'reviews' AS category, COUNT(*) AS total
  FROM journal
  WHERE createdAt >= 1772582400000 AND createdAt <= 1772668799999
  GROUP BY date
) AS a
GROUP BY date, category
ORDER BY date, category
