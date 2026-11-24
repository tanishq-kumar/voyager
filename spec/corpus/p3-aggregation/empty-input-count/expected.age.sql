SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE $p0 RETURN COUNT(n.age) AS cnt $$, %s) AS (cnt agtype)
