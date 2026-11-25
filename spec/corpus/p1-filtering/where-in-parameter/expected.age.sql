SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE n.name IN $names RETURN n.name $$, %s) AS (name agtype)
