SELECT "name" FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.name, n.age AS "_ord_0")) ORDER BY "_ord_0" ASC NULLS LAST
