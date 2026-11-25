MATCH (n:Person) RETURN COUNT(DISTINCT n.city) AS cnt
