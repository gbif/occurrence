package org.gbif.occurrence.download.service.hive;

import java.util.Arrays;
import java.util.Collection;
import org.gbif.occurrence.download.service.hive.HiveSQL.Validate.Result;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Testing validation queries.
 *
 */
@RunWith(Parameterized.class)
public class SQLValidationTest {

  private static final String COMPILATION_ERROR = "COMPILATION ERROR";


  @Parameters
  public static Collection<Object[]> inputs() {
    return Arrays.asList( new Object[][] {
      {"SELECT `gbifid`, `datasetkey`, `license` from occurrence", true, false, 0, "gbifid,datasetkey,license"}, 
      {"SELECT `gbifid`, `countrycode`, `datasetkey`, `license` from occurrence", true, false, 0, "gbifid,countrycode,datasetkey,license"},
      {"SELECT `gbifid`, `countrycode`, `datasetkey`, `license` from `occurrence` where `countrycode`='US'", true, false, 0, "gbifid,countrycode,datasetkey,license"},
      {"SELECT `gbifid`, `countrycode`, `datasetkey`, `license`, `month`, `year` FROM `occurrence` WHERE `month`=3 AND `year` = 2018", true, false, 0, "gbifid,countrycode,datasetkey,license,month,year"},
      {"SELECT COUNT(`datasetkey`), `countrycode` ,`datasetkey` ,`license` FROM `occurrence` GROUP BY `countrycode`, `license`, `datasetkey`", true, false, 0, "COUNT(`datasetkey`),countrycode,datasetkey,license"},
      {"SELECT COUNT(`datasetkey`), `countrycode` ,`datasetkey`, `license` FROM `occurrence` GROUP BY `countrycode`, `license`, `datasetkey` HAVING count(`datasetkey`) > 5", true, false, 0, "COUNT(`datasetkey`),countrycode,datasetkey,license"},
      {"SELECT col  FROM (  SELECT a+b AS col  FROM t1) t2", false, true, 5, "COL"},
      {"SELECT a.* FROM a JOIN b ON (a.id = b.id)", false, true, 4, "A.*"},
      {"SELECT key FROM (SELECT key FROM src ORDER BY key LIMIT 10) UNION SELECT key FROM (SELECT key FROM src1 ORDER BY key LIMIT 10)", false, true ,1, ""}
  });
  } 
  
  private final String query;
  private final boolean isResultOk;
  private final boolean isCompilationError;
  private final int numberOfIssues;
  private final String sqlHeader;
  
  public SQLValidationTest(String query, boolean isResultOk, boolean isCompilationError, int numberOfIssues, String sqlHeader) {
    this.query = query;
    this.isResultOk = isResultOk;
    this.isCompilationError = isCompilationError;
    this.numberOfIssues = numberOfIssues;
    this.sqlHeader = sqlHeader;
  }

  @Test
  public void testValidInvalidQueries() {
    Result result = new HiveSQL.Validate().apply(query);
    Assert.assertEquals(isResultOk, result.isOk());
    Assert.assertEquals(isCompilationError, result.explain().equals(COMPILATION_ERROR));
    Assert.assertEquals(numberOfIssues, result.issues().size());  
    Assert.assertEquals(sqlHeader, result.sqlHeader().replaceAll("\t", ",").trim());  
  }

}
