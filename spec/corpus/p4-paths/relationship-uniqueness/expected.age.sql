SELECT * FROM cypher('age_graph', $$ MATCH (x:Person)-[r1:KNOWS]-(y:Person)-[r2:KNOWS]-(z:Person) RETURN x.name AS x_name, z.name AS z_name $$) AS ("x_name" agtype, "z_name" agtype)
