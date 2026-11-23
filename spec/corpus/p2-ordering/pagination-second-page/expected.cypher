MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name ASC SKIP 2 LIMIT 2
