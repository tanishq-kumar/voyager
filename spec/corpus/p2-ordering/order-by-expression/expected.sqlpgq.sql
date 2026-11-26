SELECT "name", "age" FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.name, n.age, n.age + $p0 AS "_ord_0")) ORDER BY "_ord_0" ASC NULLS LAST
