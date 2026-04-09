
select count(*)
from postgres.comments as c,
     postgres.votes as v,
     postgres.users as u
where u.id = c.userid
  and u.id = v.userid
  and c.creationdate >= timestamp '2010-10-01 20:45:26'
  and c.creationdate <= timestamp '2014-09-05 12:51:17'
  and v.bountyamount <= 100
  and u.upvotes = 0
  and u.creationdate <= timestamp '2014-09-12 03:25:34';
