SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE (n.nickname = $p0) OR (n.age > $p1) RETURN n.name $$, %s) AS (name agtype)
