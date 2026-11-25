MATCH (n:Person) WHERE NOT (n.name IN $names) RETURN n.name
