SELECT COUNT("n") AS "cnt" FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n AS "n"))
