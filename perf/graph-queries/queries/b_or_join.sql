SELECT DISTINCT n.id
FROM node n
JOIN edge e ON (
    (e.fromId = 'f47ac10b-58cc-4372-a567-0e02b2c3d479' AND e.toId = n.id AND e.label = 'requires')
    OR (e.toId = 'f47ac10b-58cc-4372-a567-0e02b2c3d479' AND e.fromId = n.id AND e.label = 'requires')
)
WHERE n.type = 'course'
