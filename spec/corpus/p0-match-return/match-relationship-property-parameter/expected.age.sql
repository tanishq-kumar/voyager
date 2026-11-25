SELECT * FROM cypher('age_graph', $$ MATCH (a)-[r:KNOWS {since: $since}]->(b) RETURN r $$, %s) AS ("r" agtype)
