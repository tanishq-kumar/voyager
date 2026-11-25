SELECT * FROM cypher('age_graph', $$ MATCH (n) RETURN $p0 AS flag $$, %s) AS ("flag" agtype)
