SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE $p0 RETURN AVG(n.age) AS avg_age $$, %s) AS (avg_age agtype)
