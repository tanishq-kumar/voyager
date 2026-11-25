SELECT * FROM cypher('age_graph', $$ MATCH (a:Person)-[r:KNOWS*1..1]->(b:Person) WHERE a.name = $p0 RETURN b.name AS b_name $$, %s) AS (b_name agtype)
