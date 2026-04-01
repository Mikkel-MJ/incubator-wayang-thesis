select count(*) from postgres.badges as b, postgres.users as u where b.userid= u.id and u.upvotes>=0;
