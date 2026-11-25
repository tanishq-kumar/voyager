SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.active, n.age ORDER BY n.active ASC, n.age ASC $$) AS (active agtype, age agtype)
