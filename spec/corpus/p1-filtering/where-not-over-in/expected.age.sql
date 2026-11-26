SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE NOT (n.name IN $names) RETURN n.name $$, %s) AS ("name" agtype)
