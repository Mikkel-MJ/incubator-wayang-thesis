select count(*)
from postgres.comments as c,
     postgres.posthistory as ph,
     postgres.votes as v,
     postgres.users as u
where u.id = v.userid
  and v.userid = ph.userid
  and ph.userid = c.userid
  and v.bountyamount >= 0
  and v.creationdate >= timestamp '2010-07-26 00:00:00'
  and v.creationdate <= timestamp '2014-09-08 00:00:00'
  and u.reputation >= 1
  and u.views >= 0
  and u.views <= 110
  and u.upvotes = 0
  and u.creationdate >= timestamp '2010-07-28 19:29:11'
  and u.creationdate <= timestamp '2014-08-14 05:29:30';
