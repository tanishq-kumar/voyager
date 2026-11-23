SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.active, n.name ORDER BY n.active DESC, n.name ASC $$) AS (active agtype, name agtype)
