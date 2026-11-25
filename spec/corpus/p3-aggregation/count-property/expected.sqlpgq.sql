SELECT COUNT("age") AS "cnt" FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.age AS "age"))
