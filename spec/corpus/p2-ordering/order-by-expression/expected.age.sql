SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age + $p0 ASC $$, %s) AS (name agtype, age agtype)
