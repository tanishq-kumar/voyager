SELECT COALESCE(ARRAY_AGG("city") FILTER (WHERE "city" IS NOT NULL), '{}') AS "cities" FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.city AS "city"))
