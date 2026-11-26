SELECT "city", "active", COUNT("age") AS "cnt" FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.city AS "city", n.active AS "active", n.age AS "age")) GROUP BY "city", "active"
