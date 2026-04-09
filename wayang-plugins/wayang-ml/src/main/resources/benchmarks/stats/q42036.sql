select count(*)
from postgres.comments as c,
     postgres.posts as p,
     postgres.votes as v,
     postgres.users as u
where u.id = c.userid
  and u.id = p.owneruserid
  and u.id = v.userid
  and c.score = 0
  and c.creationdate <= timestamp '2014-09-13 20:12:15'
  and p.creationdate >= timestamp '2010-07-27 01:51:15'
  and v.bountyamount <= 50
  and v.creationdate <= timestamp '2014-09-12 00:00:00'
  and u.upvotes >= 0
  and u.upvotes <= 12
  and u.creationdate >= timestamp '2010-07-19 19:09:39';
