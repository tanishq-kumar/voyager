SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE n.name ENDS WITH $p0 RETURN n.name $$, %s) AS ("name" agtype)
