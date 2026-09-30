SELECT e.toId AS course_id, e.fromId AS lecture_id
FROM edge e
JOIN node n_from ON e.fromId = n_from.id
JOIN node n_to ON e.toId = n_to.id
WHERE n_from.type = 'lecture'
  AND n_from.id IN (
    '6ba7b810-9dad-41d8-80b4-00c04fd430c8',
    '6ba7b811-9dad-41d8-80b4-00c04fd430c9',
    '6ba7b812-9dad-41d8-80b4-00c04fd430ca',
    '6ba7b813-9dad-41d8-80b4-00c04fd430cb',
    '6ba7b814-9dad-41d8-80b4-00c04fd430cc',
    '6ba7b815-9dad-41d8-80b4-00c04fd430cd',
    '6ba7b816-9dad-41d8-80b4-00c04fd430ce',
    '6ba7b817-9dad-41d8-80b4-00c04fd430cf',
    '6ba7b818-9dad-41d8-80b4-00c04fd430d0',
    '6ba7b819-9dad-41d8-80b4-00c04fd430d1'
  )
  AND n_to.type = 'course'
  AND n_to.id NOT IN ('f47ac10b-58cc-4372-a567-0e02b2c3d479')
UNION ALL
SELECT e.fromId AS course_id, e.toId AS lecture_id
FROM edge e
JOIN node n_from ON e.fromId = n_from.id
JOIN node n_to ON e.toId = n_to.id
WHERE n_to.type = 'lecture'
  AND n_to.id IN (
    '6ba7b810-9dad-41d8-80b4-00c04fd430c8',
    '6ba7b811-9dad-41d8-80b4-00c04fd430c9',
    '6ba7b812-9dad-41d8-80b4-00c04fd430ca',
    '6ba7b813-9dad-41d8-80b4-00c04fd430cb',
    '6ba7b814-9dad-41d8-80b4-00c04fd430cc',
    '6ba7b815-9dad-41d8-80b4-00c04fd430cd',
    '6ba7b816-9dad-41d8-80b4-00c04fd430ce',
    '6ba7b817-9dad-41d8-80b4-00c04fd430cf',
    '6ba7b818-9dad-41d8-80b4-00c04fd430d0',
    '6ba7b819-9dad-41d8-80b4-00c04fd430d1'
  )
  AND n_from.type = 'course'
  AND n_from.id NOT IN ('f47ac10b-58cc-4372-a567-0e02b2c3d479')
