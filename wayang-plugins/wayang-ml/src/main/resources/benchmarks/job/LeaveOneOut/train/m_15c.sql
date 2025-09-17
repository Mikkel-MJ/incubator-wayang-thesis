SELECT MIN(mi.info) AS release_date,
       MIN(t.title) AS modern_american_internet_movie
FROM postgres.title AS t
INNER JOIN postgres.aka_title AS akat ON t.id = akat.movie_id
INNER JOIN postgres.movie_info AS mi ON t.id = mi.movie_id
INNER JOIN postgres.movie_keyword AS mk ON t.id = mk.movie_id
INNER JOIN postgres.movie_companies AS mc ON t.id = mc.movie_id
INNER JOIN postgres.keyword AS k ON mk.keyword_id = k.id
INNER JOIN postgres.info_type AS it1 ON mi.info_type_id = it1.id
INNER JOIN postgres.company_name AS cn ON mc.company_id = cn.id
INNER JOIN postgres.company_type AS ct ON mc.company_type_id = ct.id
WHERE cn.country_code = '[us]'
  AND it1.info = 'release dates'
  AND mi.note LIKE '%internet%'
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'USA:% 199%' OR mi.info LIKE 'USA:% 200%')
  AND t.production_year > 1990;
