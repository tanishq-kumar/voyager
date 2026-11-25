MATCH p = (a:Person)-[r1:KNOWS]->(b:Person)-[r2:KNOWS]->(c:Person) WHERE a.name = $p0 RETURN c.name AS c_name, length(p) AS len
