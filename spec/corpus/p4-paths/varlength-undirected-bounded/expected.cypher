MATCH (a:Person)-[r:KNOWS*1..2]-(b:Person) WHERE a.name = $p0 RETURN b.name AS b_name
