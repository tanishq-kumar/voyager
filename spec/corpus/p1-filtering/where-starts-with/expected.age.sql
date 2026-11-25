SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE n.name STARTS WITH $p0 RETURN n.name $$, %s) AS ("name" agtype)
