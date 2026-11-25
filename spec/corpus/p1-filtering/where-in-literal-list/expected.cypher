MATCH (n:Person) WHERE n.name IN [$p0, $p1, $p2] RETURN n.name
