MATCH p = (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = $p0 RETURN relationships(p) AS rs
