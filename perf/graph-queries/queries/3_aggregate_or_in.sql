SELECT course_id, MIN(created_date) AS firstCreated,
       MAX(created_date) AS lastCreated, COUNT(*) AS totalCount
FROM (
  SELECT n.id as course_id, n.createdAt AS created_date
  FROM node n
  WHERE (n.type = 'lecture')
    AND (
      n.id IN (
        SELECT e.fromId FROM edge e
        WHERE e.label = 'requires'
          AND e.toId = 'f47ac10b-58cc-4372-a567-0e02b2c3d479'
      )
      OR n.id IN (
        SELECT e.toId FROM edge e
        WHERE e.label = 'requires'
          AND e.fromId = 'f47ac10b-58cc-4372-a567-0e02b2c3d479'
      )
    )
) AS combined
