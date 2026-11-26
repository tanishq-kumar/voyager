SELECT * FROM cypher('age_graph', $$ MATCH (n) RETURN n $$) AS ("n" agtype)
