select count(*) from postgres.votes as v, postgres.badges as b, postgres.users as u where u.id = v.userid and v.userid = b.userid and u.downvotes>=0 and u.downvotes<=0;
