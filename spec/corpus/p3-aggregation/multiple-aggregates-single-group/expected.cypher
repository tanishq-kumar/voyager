MATCH (n:Person) RETURN COUNT(n.age) AS cnt, SUM(n.age) AS total, AVG(n.age) AS avg_age, MIN(n.age) AS min_age, MAX(n.age) AS max_age
