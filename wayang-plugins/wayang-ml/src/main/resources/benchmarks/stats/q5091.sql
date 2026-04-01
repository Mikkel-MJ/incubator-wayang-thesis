select count(*)
from postgres.tags as t,
     postgres.posts as p,
     postgres.users as u,
     postgres.votes as v,
     postgres.badges as b
where p.id = t.excerptpostid
  and u.id = v.userid
  and u.id = b.userid
  and u.id = p.owneruserid
  and u.views >= 0
  and u.views <= 515
  and u.upvotes >= 0
  and u.creationdate <= timestamp '2014-09-07 13:46:41'
  and v.bountyamount >= 0
  and v.bountyamount <= 200
  and b."date" <= timestamp '2014-09-12 12:56:22';
