SELECT * FROM cypher('age_graph', $$ MATCH p = (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = $p0 RETURN b.name AS b_name, length(p) AS len $$, %s) AS ("b_name" agtype, "len" agtype)
