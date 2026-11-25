SELECT * FROM cypher('age_graph', $$ MATCH (a)-[r]->(b) RETURN a, r, b $$) AS (a agtype, r agtype, b agtype)
