# 2024-04-02
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# Tests for the whereInterstageHeuristic() routine in the query planner.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix whereN

# The following is a simplified and "sanitized" version of the original
# real-world query that brought the problem to light.
#
# The issue is a slow query.  The answer is correct, but it was taking too
# much time, because it was doing a full table scan rather than an indexed
# lookup.
#
# The problem was that the query planner was overestimating the number of
# output rows.  The estimated number of output rows is accurate if the
# DSNAME parameter is "ds-one".  In that case, a large fraction of the rows
# in "violation" end up being output.  The query planner correctly deduces
# that it is faster to do a full table scan of the large "violation" table
# to avoid the after-query sort that implements the ORDER BY clause. However,
# if the DSNAME is "ds-two", then only a few rows (about 6) are generated,
# and it is much much faster to do an indexed lookup of "violation" followed
# by a sort operation to implement ORDER BY
#
# The problem, of course, is that the query planner has no way of knowing
# in advance how many rows will be generated.  The query planner tries to
# estimate a worst case, which is a large number of output rows, and it picks
# the best plan for that case.  However, the plan choosen is very inefficient
# when the number of output rows is small.
#
# The whereInterstageHeuristic() routine in the query planner attempts to
# correct this by adjusting the query plan such that it avoids the very bad
# query plan for a small number of rows, at the expense of a slightly less
# efficient plan for a large number of rows.  The large number of rows case
# is perhaps 5% slower with the revised plan, but the small number of
# rows case is around 100 times faster.  That seems like a good tradeoff.
#
do_execsql_test 1.0 {
  CREATE TABLE datasource(dsid INT, name TEXT);
  INSERT INTO datasource VALUES(1,'ds-one'),(2,'ds-two'),(3,'ds-three');
  CREATE INDEX ds1 ON datasource(name, dsid);

  CREATE TABLE rule(rid INT, team_id INT, dsid INT);
  WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<9)
    INSERT INTO rule(rid,team_id,dsid) SELECT n, 1, 1 FROM c;
  WITH RECURSIVE c(n) AS (VALUES(10) UNION ALL SELECT n+1 FROM c WHERE n<24)
    INSERT INTO rule(rid,team_id,dsid) SELECT n, 2, 2 FROM c;
  CREATE INDEX rule2 ON rule(dsid, rid);

  CREATE TABLE violation(vid INT, rid INT, vx BLOB);
  /***  Uncomment to insert actual data
  WITH src(rid, cnt) AS (VALUES(1,3586),(2,1343),(3,6505),(5,76230),
                               (6,740),(7,287794),(8,457),(12,1),
                               (14,1),(16,1),(17,1),(18,1),(19,1))
    INSERT INTO violation(vid, rid, vx)
      SELECT rid*1000000+value, rid, randomblob(15)
        FROM src, generate_series(1,cnt);
  ***/
  CREATE INDEX v1 ON violation(rid, vid);
  CREATE INDEX v2 ON violation(vid);
  ANALYZE;
  DELETE FROM sqlite_stat1;
  DROP TABLE IF EXISTS sqlite_stat4;
  INSERT INTO sqlite_stat1 VALUES
    ('violation','v2','376661 1'),
    ('violation','v1','376661 28974 1'),
    ('rule','rule2','24 12 1'),
    ('datasource','ds1','3 1 1');
  ANALYZE sqlite_schema;
}
set DSNAME ds-two   ;#  Only a few rows.  Change to "ds-one" for many rows.
do_eqp_test 1.1 {
  SELECT count(*), length(group_concat(vx)) FROM (
    SELECT V.*
      FROM datasource DS, rule R, violation V
     WHERE V.rid=R.rid
       AND R.dsid=DS.dsid
       AND DS.name=$DSNAME
     ORDER BY V.vid desc
  );
} {
  QUERY PLAN
  |--CO-ROUTINE (subquery-xxxxxx)
  |  |--SEARCH DS USING COVERING INDEX ds1 (name=?)
  |  |--SEARCH R USING COVERING INDEX rule2 (dsid=?)
  |  |--SEARCH V USING INDEX v1 (rid=?)
  |  `--USE TEMP B-TREE FOR ORDER BY
  `--SCAN (subquery-xxxxxx)
}
#       ^^^^---- We want to see three SEARCH terms.  No SCAN terms.
#                The ORDER BY is implemented by a separate sorter pass.

finish_test
