SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE $p0 RETURN MIN(n.age) AS min_age, MAX(n.age) AS max_age $$, %s) AS (min_age agtype, max_age agtype)
