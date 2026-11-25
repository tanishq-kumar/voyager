MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name ASC SKIP 4 LIMIT 2
