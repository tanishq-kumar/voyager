SELECT * FROM cypher('age_graph', $$ MATCH (a:A)<-[:R]-(b:B) RETURN a, b $$) AS ("a" agtype, "b" agtype)
