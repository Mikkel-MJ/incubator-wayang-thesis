select count(*) from postgres.comments as c, postgres.posthistory as ph where c.userid = ph.userid and ph.posthistorytypeid=1 and ph.creationdate>=timestamp '2010-09-14 11:59:07';
