SELECT * FROM cypher('age_graph', $$ MATCH (:A)-[r]->(:B) RETURN r $$) AS ("r" agtype)
