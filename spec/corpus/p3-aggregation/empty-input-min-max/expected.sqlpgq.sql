SELECT MIN("age") AS "min_age", MAX("age") AS "max_age" FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE $p0 COLUMNS (n.age AS "age"))
