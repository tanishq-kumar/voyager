SELECT * FROM cypher('age_graph', $$ MATCH (node)-[r:KNOWS {name: $p0}]->(a) RETURN a $$, %s) AS (a agtype)
