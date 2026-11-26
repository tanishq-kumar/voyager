SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name ASC SKIP 0 LIMIT 2 $$) AS ("name" agtype, "age" agtype)
