SELECT * FROM cypher('age_graph', $$ MATCH p = (a:Person) WHERE a.name = $p0 RETURN a.name AS a_name, length(p) AS len $$, %s) AS (a_name agtype, len agtype)
