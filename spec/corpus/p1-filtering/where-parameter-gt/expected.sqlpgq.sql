SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE n.age > $age COLUMNS (n.name))
