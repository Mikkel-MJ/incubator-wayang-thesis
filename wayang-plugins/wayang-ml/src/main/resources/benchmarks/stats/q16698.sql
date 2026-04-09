select count(*)
from postgres.comments as c,
     postgres.posts as p,
     postgres.posthistory as ph,
     postgres.badges as b,
     postgres.users as u
where u.id = ph.userid
  and u.id = b.userid
  and u.id = p.owneruserid
  and u.id = c.userid
  and c.score = 0
  and p.score >= -2
  and p.commentcount >= 0
  and p.commentcount <= 12
  and p.favoritecount >= 0
  and p.favoritecount <= 6
  and ph.creationdate <= timestamp '2014-08-18 08:54:12'
  and u.views = 0
  and u.downvotes >= 0
  and u.downvotes <= 60;
