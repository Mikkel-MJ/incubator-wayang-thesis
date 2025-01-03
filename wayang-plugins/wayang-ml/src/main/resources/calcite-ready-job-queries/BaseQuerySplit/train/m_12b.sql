SELECT MIN(mi.info) AS budget,
       MIN(t.title) AS unsuccessful_movie
FROM postgres.company_name AS cn
INNER JOIN postgres.movie_companies AS mc ON cn.id = mc.company_id
INNER JOIN postgres.company_type AS ct ON mc.company_type_id = ct.id
INNER JOIN postgres.title AS t ON mc.movie_id = t.id
INNER JOIN postgres.movie_info AS mi ON t.id = mi.movie_id
INNER JOIN postgres.movie_info_idx AS mi_idx ON t.id = mi_idx.movie_id
INNER JOIN postgres.info_type AS it1 ON mi.info_type_id = it1.id
INNER JOIN postgres.info_type AS it2 ON mi_idx.info_type_id = it2.id
WHERE cn.country_code = '[us]'
  AND ct.kind IS NOT NULL
  AND (ct.kind = 'production companies' OR ct.kind = 'distributors')
  AND it1.info = 'budget'
  AND it2.info = 'bottom 10 rank'
  AND t.production_year > 2000
  AND (t.title LIKE 'Birdemic%' OR t.title LIKE '%Movie%');
