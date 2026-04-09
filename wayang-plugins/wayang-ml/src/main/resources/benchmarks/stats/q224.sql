select count(*)
from postgres.comments as c,
     postgres.posthistory as ph,
     postgres.badges as b,
     postgres.votes as v,
     postgres.users as u
where u.id = b.userid
  and b.userid = ph.userid
  and ph.userid = v.userid
  and v.userid = c.userid
  and c.creationdate >= timestamp '2010-07-20 21:37:31'
  and ph.posthistorytypeid = 12
  and u.upvotes = 0;
