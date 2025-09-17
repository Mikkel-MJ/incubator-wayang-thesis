SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS western_violent_movie
FROM postgres.company_name AS cn
INNER JOIN postgres.movie_companies AS mc ON cn.id = mc.company_id
INNER JOIN postgres.company_type AS ct ON mc.company_type_id = ct.id
INNER JOIN postgres.movie_info AS mi ON mc.movie_id = mi.movie_id
INNER JOIN postgres.title AS t ON mi.movie_id = t.id
INNER JOIN postgres.movie_keyword AS mk ON t.id = mk.movie_id
INNER JOIN postgres.keyword AS k ON mk.keyword_id = k.id
INNER JOIN postgres.movie_info_idx AS mi_idx ON t.id = mi_idx.movie_id
INNER JOIN postgres.info_type AS it1 ON mi.info_type_id = it1.id
INNER JOIN postgres.info_type AS it2 ON mi_idx.info_type_id = it2.id
INNER JOIN postgres.kind_type AS kt ON t.kind_id = kt.id
WHERE cn.country_code <> '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder', 'murder-in-title', 'blood', 'violence')
  AND kt.kind IN ('movie', 'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA', 'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2005;
