SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE (n.city = $p0) OR (n.age > $p1) RETURN n.name $$, %s) AS (name agtype)
