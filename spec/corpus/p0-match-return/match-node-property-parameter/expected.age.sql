SELECT * FROM cypher('age_graph', $$ MATCH (n:Person {name: $name}) RETURN n $$, %s) AS ("n" agtype)
