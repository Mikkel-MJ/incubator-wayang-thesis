SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS mainstream_movie
FROM postgres.company_name AS cn
INNER JOIN postgres.movie_companies AS mc ON cn.id = mc.company_id
INNER JOIN postgres.company_type AS ct ON ct.id = mc.company_type_id
INNER JOIN postgres.title AS t ON t.id = mc.movie_id
INNER JOIN postgres.movie_info AS mi ON t.id = mi.movie_id
INNER JOIN postgres.info_type AS it1 ON mi.info_type_id = it1.id
INNER JOIN postgres.movie_info_idx AS mi_idx ON t.id = mi_idx.movie_id
INNER JOIN postgres.info_type AS it2 ON mi_idx.info_type_id = it2.id
WHERE cn.country_code = '[us]'
  AND ct.kind = 'production companies'
  AND it1.info = 'genres'
  AND it2.info = 'rating'
  AND mi.info IN ('Drama', 'Horror', 'Western', 'Family')
  AND mi_idx.info > '7.0'
  AND t.production_year BETWEEN 2000 AND 2010;
