SELECT COALESCE(ARRAY_AGG("name") FILTER (WHERE "name" IS NOT NULL), '{}') AS "names" FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.name AS "name"))
