SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.name AS person_name, n.age AS person_age)) ORDER BY person_age ASC NULLS LAST
