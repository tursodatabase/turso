WITH enrollment_dates AS (
  SELECT DISTINCT date(datetime(stamp / 1000, 'unixepoch', '+60 minutes')) AS enroll_date
  FROM (
    SELECT id, createdAt AS stamp FROM message WHERE 1=1 AND author='student'
    UNION ALL
    SELECT id, startDate AS stamp FROM conversation WHERE 1=1
    UNION ALL
    SELECT id, createdAt AS stamp FROM node WHERE 1=1 AND (type='exam' OR type='assignment')
    UNION ALL
    SELECT id, createdAt AS stamp FROM node WHERE 1=1 AND type='assignment'
    UNION ALL
    SELECT id, createdAt AS stamp FROM node WHERE 1=1 AND type='exam'
    UNION ALL
    SELECT id, createdAt AS stamp FROM journal WHERE 1=1
  )
  ORDER BY enroll_date
),
date_gaps AS (
  SELECT enroll_date,
    (
      SELECT MAX(d2.enroll_date)
      FROM enrollment_dates d2
      WHERE d2.enroll_date < d1.enroll_date
    ) AS prev_date
  FROM enrollment_dates d1
),
streaks AS (
  SELECT enroll_date, prev_date, JULIANDAY(enroll_date) - JULIANDAY(prev_date) AS diff,
    CASE WHEN JULIANDAY(enroll_date) - JULIANDAY(prev_date) = 1 THEN 0 ELSE 1 END AS streak_start
  FROM date_gaps
),
streak_ids AS (
  SELECT enroll_date, SUM(streak_start) OVER (ORDER BY enroll_date) AS streak_id
  FROM streaks
),
streak_lengths AS (
  SELECT streak_id, MIN(enroll_date) AS start_date, MAX(enroll_date) AS end_date,
         COUNT(*) AS streak_length
  FROM streak_ids
  GROUP BY streak_id
)
SELECT start_date, end_date, streak_length
FROM streak_lengths
WHERE streak_id = (
  SELECT streak_id
  FROM streak_ids
  WHERE enroll_date = (SELECT MAX(enroll_date) FROM enrollment_dates)
)
