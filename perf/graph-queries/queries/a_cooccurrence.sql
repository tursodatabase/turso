SELECT DISTINCT
  c1.displayLabel AS "from",
  c1.id AS "fromId",
  c1.type AS "fromType",
  json_extract(c1.entity, '$.level') AS "fromLevel",
  c2.displayLabel AS "to",
  c2.id AS "toId",
  c2.type AS "toType",
  json_extract(c2.entity, '$.level') AS "toLevel",
  '' AS "type"
FROM edge e1
JOIN edge e2 ON e1.fromId = e2.fromId
JOIN node c1 ON e1.toId = c1.id
JOIN node c2 ON e2.toId = c2.id
JOIN node lec ON e1.fromId = lec.id
LEFT JOIN edge eq1 ON c1.id = eq1.toId AND eq1.label = 'equivalent'
LEFT JOIN edge eq2 ON c2.id = eq2.toId AND eq2.label = 'equivalent'
WHERE e1.label = 'requires'
  AND e2.label = 'requires'
  AND e1.toId != e2.toId
  AND e1.toId < e2.toId
  AND lec.type = 'lecture'
  AND c1.type NOT IN ('semester', 'term', 'lecture')
  AND c2.type NOT IN ('semester', 'term', 'lecture')
  AND eq1.id IS NULL
  AND eq2.id IS NULL
