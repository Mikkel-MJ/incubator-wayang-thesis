select count(*) from postgres.comments as c, postgres.posthistory as ph where c.userid = ph.userid and c.score=0 and ph.posthistorytypeid=1;
