SELECT * FROM cypher('age_graph', $$ MATCH (a:Person)-[r1:KNOWS]->(b:Person)<-[r2:KNOWS]-(c:Person) RETURN a.name AS a_name, c.name AS c_name $$) AS (a_name agtype, c_name agtype)
