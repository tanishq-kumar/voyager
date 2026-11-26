SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE NOT (n.city = $p0) RETURN n.name $$, %s) AS ("name" agtype)
