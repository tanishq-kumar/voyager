MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = $p0 RETURN b.name AS b_name
