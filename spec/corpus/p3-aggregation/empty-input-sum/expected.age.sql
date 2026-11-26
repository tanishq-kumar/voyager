SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE $p0 RETURN coalesce(sum(n.age), 0) AS total_age $$, %s) AS ("total_age" agtype)
