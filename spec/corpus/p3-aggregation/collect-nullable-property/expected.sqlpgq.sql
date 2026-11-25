SELECT ARRAY_AGG(city) AS cities FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.city AS city))
