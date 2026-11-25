MATCH (n:Person) WHERE (n.age >= $p0) XOR (n.city = $p1) RETURN n.name
