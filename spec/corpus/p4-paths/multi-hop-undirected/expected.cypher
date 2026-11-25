MATCH (a:Person)-[r1:KNOWS]-(b:Person)-[r2:KNOWS]-(c:Person) WHERE a.name = $p0 RETURN b.name AS b_name, c.name AS c_name
