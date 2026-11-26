SELECT COALESCE(SUM("age"), 0) AS "total_age" FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE $p0 COLUMNS (n.age AS "age"))
