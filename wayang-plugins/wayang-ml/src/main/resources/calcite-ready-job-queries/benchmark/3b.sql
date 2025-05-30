SELECT MIN(t.title) AS movie_title
FROM postgres.keyword AS k,
     postgres.movie_info AS mi,
     postgres.movie_keyword AS mk,
     postgres.title AS t
WHERE k.keyword LIKE '%sequel%'
  AND mi.info IN ('Bulgaria')
  AND t.production_year > 2010
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi.movie_id
  AND k.id = mk.keyword_id;

