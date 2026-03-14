SELECT n.id, n.displayLabel, n.type, n.createdAt, n.entity,
  (IFNULL(edge_from.edge_count_from, 0) + IFNULL(edge_to.edge_count_to, 0)) AS total_edges,
  e_eq.fromId as equivalentOf
FROM node as n
LEFT JOIN edge e_eq_ex ON n.id = e_eq_ex.toId AND e_eq_ex.label = 'equivalent'
LEFT JOIN (
  SELECT fromId, COUNT(*) AS edge_count_from FROM edge GROUP BY fromId
) AS edge_from ON n.id = edge_from.fromId
LEFT JOIN (
  SELECT toId, COUNT(*) AS edge_count_to FROM edge GROUP BY toId
) AS edge_to ON n.id = edge_to.toId
LEFT JOIN edge e_eq ON n.id = e_eq.toId AND e_eq.label = 'equivalent'
WHERE json_extract(n.entity, '$.level') = 'introductory'
  AND n.type = 'course'
  AND e_eq_ex.id IS NULL
ORDER BY total_edges DESC, n.createdAt DESC
LIMIT 5
