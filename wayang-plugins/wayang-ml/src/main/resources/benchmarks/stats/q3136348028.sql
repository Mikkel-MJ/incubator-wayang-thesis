select count(*) from postgres.posthistory as ph, postgres.votes as v, postgres.users as u, postgres.badges as b where u.id = b.userid and u.id = ph.userid and u.id = v.userid and u.views>=0;
