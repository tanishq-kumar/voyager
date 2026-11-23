SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.name, n.age + $p0 AS age_plus_one ORDER BY age_plus_one ASC $$, %s) AS (name agtype, age_plus_one agtype)
