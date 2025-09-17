SELECT MIN(cn.name) AS from_company,
       MIN(lt.link) AS movie_link_type,
       MIN(t.title) AS sequel_movie
FROM postgres.company_name AS cn
INNER JOIN postgres.movie_companies AS mc ON cn.id = mc.company_id
INNER JOIN postgres.company_type AS ct ON mc.company_type_id = ct.id
INNER JOIN postgres.title AS t ON mc.movie_id = t.id
INNER JOIN postgres.movie_link AS ml ON t.id = ml.movie_id
INNER JOIN postgres.link_type AS lt ON ml.link_type_id = lt.id
INNER JOIN postgres.movie_keyword AS mk ON t.id = mk.movie_id
INNER JOIN postgres.keyword AS k ON mk.keyword_id = k.id
WHERE cn.country_code <> '[pl]'
  AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%')
  AND ct.kind = 'production companies'
  AND k.keyword = 'sequel'
  AND lt.link LIKE '%follows%'
  AND mc.note IS NULL
  AND t.production_year = 1998
  AND t.title LIKE '%Money%';
