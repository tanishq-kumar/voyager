SELECT * FROM cypher('age_graph', $$ MATCH (a)-[r:REL {name: $p0}]-(b) RETURN r.name $$, %s) AS (name agtype)
