SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE $p0 RETURN n.city AS city, COUNT(n.age) AS cnt $$, %s) AS (city agtype, cnt agtype)
