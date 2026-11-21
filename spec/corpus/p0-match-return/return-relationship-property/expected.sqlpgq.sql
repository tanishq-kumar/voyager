SELECT * FROM GRAPH_TABLE (graph_table MATCH (a) -[r IS "REL" WHERE r.name = $p0]- (b) COLUMNS (r.name))
