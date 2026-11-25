MATCH p = (a:Person) WHERE a.name = $p0 RETURN a.name AS a_name, length(p) AS len
