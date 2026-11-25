SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE n.age >= $p0 RETURN n.name $$, %s) AS ("name" agtype)
