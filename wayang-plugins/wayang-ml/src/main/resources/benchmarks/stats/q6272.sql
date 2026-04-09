select count(*)
from postgres.posthistory as ph,
     postgres.votes as v,
     postgres.users as u,
     postgres.badges as b
where u.id = ph.userid
  and u.id = v.userid
  and u.id = b.userid
  and ph.posthistorytypeid = 2
  and u.views = 5
  and u.downvotes >= 0
  and u.upvotes >= 0
  and u.upvotes <= 224
  and u.creationdate <= timestamp '2014-09-04 04:41:22'
  and b."date" >= timestamp '2010-07-19 19:39:10'
  and b."date" <= timestamp '2014-09-05 18:37:48';
