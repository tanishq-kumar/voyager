SELECT * FROM cypher('age_graph', $$ MATCH (a)-[r {name: $p0}]-(b) RETURN a, b $$, %s) AS (a agtype, b agtype)
