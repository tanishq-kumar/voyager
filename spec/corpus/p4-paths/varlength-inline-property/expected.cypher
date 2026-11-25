MATCH (a:Person)-[r:KNOWS*1..2 {weight: $p0}]->(b:Person) WHERE a.name = $p1 RETURN b.name AS b_name
