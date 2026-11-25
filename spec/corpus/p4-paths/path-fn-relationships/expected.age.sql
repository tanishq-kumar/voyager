SELECT * FROM cypher('age_graph', $$ MATCH p = (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = $p0 RETURN relationships(p) AS rs $$, %s) AS (rs agtype)
