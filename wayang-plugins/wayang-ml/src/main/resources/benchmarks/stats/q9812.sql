select count(*)
from postgres.comments as c,
     postgres.posts as p,
     postgres.votes as v,
     postgres.badges as b,
     postgres.users as u
where u.id = c.userid
  and c.userid = p.owneruserid
  and p.owneruserid = v.userid
  and v.userid = b.userid
  and c.score = 1
  and p.score >= -2
  and p.score <= 23
  and p.viewcount <= 2432
  and p.commentcount = 0
  and p.favoritecount >= 0
  and u.reputation >= 1
  and u.reputation <= 113
  and u.views >= 0
  and u.views <= 51;
