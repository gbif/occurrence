-- Test changes to date interpretation using the DateParseUDF
--

-- Note this is slow and ties up YARN resources for a while.
CREATE TABLE matt.distincteventdates STORED AS ORC AS
  SELECT COUNT(*) AS c, datasetkey, v_eventdate, v_year, v_month, v_day, v_startdayofyear, v_enddayofyear
  FROM prod_h.occurrence
  GROUP BY datasetkey, v_eventdate, v_year, v_month, v_day, v_startdayofyear, v_enddayofyear;

ADD JAR hdfs://ha-nn/user/mblissett/occurrence-hive-0.195.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION parseDate AS 'org.gbif.occurrence.hive.udf.DateParseUDF';

DROP TABLE matt.intepreteddistincteventdates;
CREATE TABLE matt.intepreteddistincteventdates STORED AS ORC AS SELECT
  c, datasetkey, d.year, d.month, d.day, from_unixtime(floor(d.epoch_from/1000)), from_unixtime(floor(d.epoch_to/1000)), d.issue,
  v_eventdate, v_year, v_month, v_day, v_startdayofyear, v_enddayofyear
FROM
  (SELECT
     c, datasetkey, v_eventdate, v_year, v_month, v_day, v_startdayofyear, v_enddayofyear,
     parseDate(v_year, v_month, v_day, v_eventdate, v_startdayofyear, v_enddayofyear) d
   FROM matt.distincteventdates
  ) r;
