select count(*)
from postgres.votes as v,
     postgres.badges as b,
     postgres.users as u
where u.id = v.userid
  and v.userid = b.userid
  and v.bountyamount >= 0
  and v.bountyamount <= 50
  and u.downvotes = 0;
