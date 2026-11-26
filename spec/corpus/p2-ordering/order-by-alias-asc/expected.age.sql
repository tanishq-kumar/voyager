SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.name AS person_name, n.age AS person_age ORDER BY person_age ASC $$) AS ("person_name" agtype, "person_age" agtype)
