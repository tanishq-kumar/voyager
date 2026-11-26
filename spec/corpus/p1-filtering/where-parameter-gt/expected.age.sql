SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE n.age > $age RETURN n.name $$, %s) AS ("name" agtype)
