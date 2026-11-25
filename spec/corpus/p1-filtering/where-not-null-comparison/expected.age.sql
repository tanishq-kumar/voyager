SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE NOT (n.nickname = $p0) RETURN n.name $$, %s) AS ("name" agtype)
