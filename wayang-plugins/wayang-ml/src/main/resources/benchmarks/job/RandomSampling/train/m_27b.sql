SELECT MIN(cn.name) AS producing_company,
       MIN(lt.link) AS link_type,
       MIN(t.title) AS complete_western_sequel
FROM postgres.complete_cast AS cc
INNER JOIN postgres.comp_cast_type AS cct1 ON cct1.id = cc.subject_id
INNER JOIN postgres.comp_cast_type AS cct2 ON cct2.id = cc.status_id
INNER JOIN postgres.company_name AS cn ON cn.id = mc.company_id
INNER JOIN postgres.company_type AS ct ON ct.id = mc.company_type_id
INNER JOIN postgres.keyword AS k ON k.id = mk.keyword_id
INNER JOIN postgres.link_type AS lt ON lt.id = ml.link_type_id
INNER JOIN postgres.movie_companies AS mc ON mc.movie_id = t.id
INNER JOIN postgres.movie_info AS mi ON mi.movie_id = t.id
INNER JOIN postgres.movie_keyword AS mk ON mk.movie_id = t.id
INNER JOIN postgres.movie_link AS ml ON ml.movie_id = t.id
INNER JOIN postgres.title AS t ON t.id = cc.movie_id
WHERE cct1.kind IN ('cast', 'crew')
  AND cct2.kind = 'complete'
  AND cn.country_code <> '[pl]'
  AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%')
  AND ct.kind = 'production companies'
  AND k.keyword = 'sequel'
  AND lt.link LIKE '%follow%'
  AND mc.note IS NULL
  AND mi.info IN ('Sweden', 'Germany', 'Swedish', 'German')
  AND t.production_year = 1998;
