SELECT * FROM cypher('age_graph', $$ MATCH (n) RETURN $p0 AS one $$, %s) AS ("one" agtype)
