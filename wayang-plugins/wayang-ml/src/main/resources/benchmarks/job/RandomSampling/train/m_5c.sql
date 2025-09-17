SELECT MIN(t.title) AS american_movie
FROM postgres.title AS t
INNER JOIN postgres.movie_info AS mi ON t.id = mi.movie_id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.company_type AS ct ON ct.id = mc.company_type_id
INNER JOIN postgres.info_type AS it ON it.id = mi.info_type_id
WHERE ct.kind = 'production companies'
  AND mc.note NOT LIKE '%(TV)%'
  AND mc.note LIKE '%(USA)%'
  AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'USA', 'American')
  AND t.production_year > 1990;
