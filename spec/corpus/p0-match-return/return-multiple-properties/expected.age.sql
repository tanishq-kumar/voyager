SELECT * FROM cypher('age_graph', $$ MATCH (a) RETURN a.name, a.age, a.seasons $$) AS ("name" agtype, "age" agtype, "seasons" agtype)
