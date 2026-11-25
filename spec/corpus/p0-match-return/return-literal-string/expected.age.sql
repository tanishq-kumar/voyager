SELECT * FROM cypher('age_graph', $$ MATCH (n) RETURN $p0 AS greeting $$, %s) AS (greeting agtype)
