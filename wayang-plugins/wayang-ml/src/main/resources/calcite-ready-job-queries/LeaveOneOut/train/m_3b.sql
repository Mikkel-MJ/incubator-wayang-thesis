SELECT MIN(t.title) AS movie_title
FROM postgres.keyword AS k
INNER JOIN postgres.movie_keyword AS mk ON k.id = mk.keyword_id
INNER JOIN postgres.movie_info AS mi ON mk.movie_id = mi.movie_id
INNER JOIN postgres.title AS t ON t.id = mi.movie_id
WHERE k.keyword LIKE '%sequel%'
  AND mi.info IN ('Bulgaria')
  AND t.production_year > 2010;
