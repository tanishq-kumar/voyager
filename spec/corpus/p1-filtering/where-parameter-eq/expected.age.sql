SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE n.name = $name RETURN n.name $$, %s) AS (name agtype)
