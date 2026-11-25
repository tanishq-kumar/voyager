MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name ASC SKIP 10 LIMIT 2
