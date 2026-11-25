SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.name, n.city ORDER BY n.city DESC, n.name ASC $$) AS (name agtype, city agtype)
