MATCH (n:Person) RETURN COLLECT(n.city) AS cities
