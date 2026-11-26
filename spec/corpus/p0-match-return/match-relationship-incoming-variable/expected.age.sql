SELECT * FROM cypher('age_graph', $$ MATCH (a:A)<-[r:R]-(b:B) RETURN a, r, b $$) AS ("a" agtype, "r" agtype, "b" agtype)
