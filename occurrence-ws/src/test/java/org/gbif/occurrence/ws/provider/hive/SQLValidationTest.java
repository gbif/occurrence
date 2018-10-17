package org.gbif.occurrence.ws.provider.hive;

import java.util.Arrays;
import java.util.Collection;
import org.gbif.occurrence.ws.provider.hive.HiveSQL.Validate.Result;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Testing validation queries
 *
 */
@RunWith(Parameterized.class)
public class SQLValidationTest {

  private static final String COMPILATION_ERROR = "COMPILATION ERROR";


  @Parameters
  public static Collection<Object[]> inputs() {
    return Arrays.asList( new Object[][] {
      {"SELECT gbifid, datasetkey, license from occurrence", true, false, 0}, 
      {"SELECT * from occurrence", true, false, 0},
      {"SELECT * from occurrence where countrycode='US'", true, false, 0},
      {"SELECT COUNT(datasetkey), countrycode ,datasetkey ,license FROM occurrence GROUP BY countrycode, license, datasetkey", true, false, 0},
      {"SELECT COUNT(datasetkey), countrycode ,datasetkey, license FROM occurrence GROUP BY countrycode, license, datasetkey HAVING count(datasetkey) > 5", true, false, 0},
      {"SELECT col  FROM (  SELECT a+b AS col  FROM t1) t2", false, true, 5},
      {"SELECT a.* FROM a JOIN b ON (a.id = b.id)", false, true, 4},
      {"SELECT key FROM (SELECT key FROM src ORDER BY key LIMIT 10) UNION SELECT key FROM (SELECT key FROM src1 ORDER BY key LIMIT 10)", false, true ,1}
  });
  } 
  
  private String query;
  private boolean isResultOk;
  private boolean isCompilationError;
  private int numberOfIssues;
  
  public SQLValidationTest(String query, boolean isResultOk, boolean isCompilationError, int numberOfIssues) {
    super();
    this.query = query;
    this.isResultOk = isResultOk;
    this.isCompilationError = isCompilationError;
    this.numberOfIssues = numberOfIssues;
  }

  @Test
  public void testValidInvalidQueries() {
    Result result = new HiveSQL.Validate().apply(query);
    Assert.assertEquals(isResultOk, result.isOk());
    Assert.assertEquals(isCompilationError, result.explain().equals(COMPILATION_ERROR));
    Assert.assertEquals(numberOfIssues, result.issues().size());  
  }

}
