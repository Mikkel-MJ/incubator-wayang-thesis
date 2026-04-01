select count(*)
from postgres.tags as t,
     postgres.posts as p,
     postgres.users as u,
     postgres.posthistory as ph,
     postgres.badges as b
where p.id = t.excerptpostid
  and u.id = ph.userid
  and u.id = b.userid
  and u.id = p.owneruserid
  and p.commentcount >= 0
  and u.downvotes <= 0
  and b."date" <= timestamp '2014-08-22 02:21:55';
