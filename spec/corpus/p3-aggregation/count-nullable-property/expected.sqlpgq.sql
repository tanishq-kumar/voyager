SELECT COUNT("nickname") AS "cnt" FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.nickname AS "nickname"))
