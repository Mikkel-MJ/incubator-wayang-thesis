SELECT MIN(chn.name) AS cha,
       MIN(t.title) AS russian_mov_with_actor_producer
FROM postgres.char_name AS chn,
     postgres.cast_info AS ci,
     postgres.company_name AS cn,
     postgres.company_type AS ct,
     postgres.movie_companies AS mc,
     postgres.role_type AS rt,
     postgres.title AS t
WHERE ci.note LIKE '%(producer)%'
  AND cn.country_code = '[ru]'
  AND rt.role = 'actor'
  AND t.production_year > 2010
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mc.movie_id
  AND chn.id = ci.person_role_id
  AND rt.id = ci.role_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;

