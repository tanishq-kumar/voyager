SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE $p0 RETURN SUM(n.age) AS total_age $$, %s) AS (total_age agtype)
