SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.name, n.nickname ORDER BY n.nickname DESC, n.name ASC $$) AS ("name" agtype, "nickname" agtype)
