SELECT ARRAY_AGG(name) AS names FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.name AS name))
