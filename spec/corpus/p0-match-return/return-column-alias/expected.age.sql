SELECT * FROM cypher('age_graph', $$ MATCH (a) RETURN a AS ColumnName $$) AS (ColumnName agtype)
