SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE n.name IN [$p0, $p1, $p2] RETURN n.name $$, %s) AS ("name" agtype)
