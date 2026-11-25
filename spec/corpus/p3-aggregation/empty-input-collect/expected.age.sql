SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE $p0 RETURN COLLECT(n.age) AS ages $$, %s) AS ("ages" agtype)
