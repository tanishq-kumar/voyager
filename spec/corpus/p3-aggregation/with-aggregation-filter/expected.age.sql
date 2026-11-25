SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WITH n.city AS city, COUNT(n.age) AS cnt WHERE cnt > $p0 RETURN city, cnt $$, %s) AS (city agtype, cnt agtype)
