SELECT * FROM cypher('age_graph', $$ MATCH (n {name: $p0}) RETURN n $$, %s) AS ("n" agtype)
