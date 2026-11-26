SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE n.name CONTAINS $p0 RETURN n.name $$, %s) AS ("name" agtype)
