SELECT MIN(mi.info) AS budget,
       MIN(t.title) AS unsuccsessful_movie
FROM postgres.company_name AS cn,
     postgres.company_type AS ct,
     postgres.info_type AS it1,
     postgres.info_type AS it2,
     postgres.movie_companies AS mc,
     postgres.movie_info AS mi,
     postgres.movie_info_idx AS mi_idx,
     postgres.title AS t
WHERE cn.country_code ='[us]'
  AND ct.kind IS NOT NULL
  AND (ct.kind ='production companies'
       OR ct.kind = 'distributors')
  AND it1.info ='budget'
  AND it2.info ='bottom 10 rank'
  AND t.production_year >2000
  AND (t.title LIKE 'Birdemic%'
       OR t.title LIKE '%Movie%')
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND mi.info_type_id = it1.id
  AND mi_idx.info_type_id = it2.id
  AND t.id = mc.movie_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id;

