SELECT * FROM cypher('age_graph', $$ MATCH (p:Person {name: $p0}) RETURN p $$, %s) AS (p agtype)
