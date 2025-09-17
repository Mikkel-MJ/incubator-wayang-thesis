SELECT MIN(t.title) AS typical_european_movie
FROM postgres.title AS t
INNER JOIN postgres.movie_info AS mi ON t.id = mi.movie_id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.company_type AS ct ON ct.id = mc.company_type_id
INNER JOIN postgres.info_type AS it ON it.id = mi.info_type_id
WHERE ct.kind = 'production companies'
  AND mc.note LIKE '%(theatrical)%'
  AND mc.note LIKE '%(France)%'
  AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German')
  AND t.production_year > 2005;
