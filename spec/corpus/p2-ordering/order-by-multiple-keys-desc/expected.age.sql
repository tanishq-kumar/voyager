SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.active, n.age ORDER BY n.active DESC, n.age DESC $$) AS (active agtype, age agtype)
