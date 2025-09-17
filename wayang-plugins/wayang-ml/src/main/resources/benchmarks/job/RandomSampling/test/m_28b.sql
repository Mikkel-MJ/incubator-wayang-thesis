SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS complete_euro_dark_movie
FROM postgres.complete_cast AS cc
INNER JOIN postgres.comp_cast_type AS cct1 ON cct1.id = cc.subject_id
INNER JOIN postgres.comp_cast_type AS cct2 ON cct2.id = cc.status_id
INNER JOIN postgres.company_name AS cn ON cn.id = mc.company_id
INNER JOIN postgres.company_type AS ct ON ct.id = mc.company_type_id
INNER JOIN postgres.info_type AS it1 ON it1.id = mi.info_type_id
INNER JOIN postgres.info_type AS it2 ON it2.id = mi_idx.info_type_id
INNER JOIN postgres.keyword AS k ON k.id = mk.keyword_id
INNER JOIN postgres.kind_type AS kt ON kt.id = t.kind_id
INNER JOIN postgres.movie_companies AS mc ON mc.movie_id = t.id
INNER JOIN postgres.movie_info AS mi ON mi.movie_id = t.id
INNER JOIN postgres.movie_info_idx AS mi_idx ON mi_idx.movie_id = t.id
INNER JOIN postgres.movie_keyword AS mk ON mk.movie_id = t.id
INNER JOIN postgres.title AS t ON t.id = cc.movie_id
WHERE cct1.kind = 'crew'
  AND cct2.kind <> 'complete+verified'
  AND cn.country_code <> '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder', 'murder-in-title', 'blood', 'violence')
  AND kt.kind IN ('movie', 'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Sweden', 'Germany', 'Swedish', 'German')
  AND mi_idx.info > '6.5'
  AND t.production_year > 2005;
