package org.gbif.occurrence.download.service.hive;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.Collection;
import org.gbif.occurrence.download.service.hive.validation2.DownloadsQueryRuleBase;
import org.gbif.occurrence.download.service.hive.validation2.Hive;
import org.gbif.occurrence.download.service.hive.validation2.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation2.Hive.QueryFragments;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SQLValidationTest2 {

  private static final String COMMA = ",";

  @Parameters
  public static Collection<Object[]> inputs() {
    return Arrays.asList(new Object[][] {
        {"SELECT gbifid, datasetkey, license from occurrence", true, false, 0, "gbifid,datasetkey,license", false},
        {"SELECT gbifid, countrycode, datasetkey, license from occurrence", true, false, 0, "gbifid,countrycode,datasetkey,license", false},
        {"SELECT gbifid, countrycode, datasetkey, license from occurrence where countrycode='US'", true, false, 0, "gbifid,countrycode,datasetkey,license", false},
        {"SELECT gbifid, countrycode, datasetkey, license, month, year FROM occurrence WHERE month=3 AND year = 2018", true, false, 0,"gbifid,countrycode,datasetkey,license,month,year", false},
        {"SELECT COUNT(datasetkey), countrycode ,datasetkey ,license FROM occurrence GROUP BY countrycode, license, datasetkey", true,false, 0, "COUNT (datasetkey),countrycode,datasetkey,license", true},
        {"SELECT 5*COUNT(datasetkey) count, countrycode ,datasetkey ,license FROM occurrence GROUP BY countrycode, license, datasetkey", true,false, 0, "count,countrycode,datasetkey,license", true},
        {"SELECT START(COUNT(datasetkey)), countrycode ,datasetkey ,license FROM occurrence GROUP BY countrycode, license, datasetkey", true,false, 0, "START (COUNT (datasetkey)),countrycode,datasetkey,license", true},
        {"SELECT START(COUNT(datasetkey, age)), countrycode ,datasetkey ,license FROM occurrence GROUP BY countrycode, license, datasetkey", true,false, 0, "START (COUNT (datasetkey ,age)),countrycode,datasetkey,license", true},        
        {"SELECT gbifid, countrycode, datasetkey, license FROM occurrence WHERE month=3 or (UPPER(datasetkey)='SAME' and year=2004)", true,false, 0, "gbifid,countrycode,datasetkey,license", false},
        {"SELECT COUNT(datasetkey) count, countrycode country,datasetkey ,license licenseType FROM occurrence  WHERE month=3 AND year = 2018 GROUP BY countrycode, license, datasetkey", true, false, 0, "count,country,datasetkey,licenseType", true},
        {"SELECT COUNT(datasetkey), countrycode ,datasetkey, license FROM occurrence GROUP BY countrycode, license, datasetkey HAVING count(datasetkey) > 5", false, false, 1, "", false},
        {"SELECT loc, cnt FROM (select a.loc as loc, a.cnt13 cnt from crimeloc13 a UNION ALL select b.loc as loc, b.cnt14 as cnt from crimeloc14 b ) a",false, false, 3, "", false},
        {"SELECT max(year) FROM occurrence GROUP BY year HAVING year > 2000", false, false, 1, "", true},
        {"SELECT COUNT(countrycode) CODE FROM occurrence_hdfs WHERE month IN (Select month from occurrence)  and year=2004 HAVING countrycode='CO'",false, false, 3, "", true},
        {"SELECT * from occurrence", false, false, 1, "", false}});
  }

  private final String query;
  private final boolean isResultOk;
  private final boolean parseError;
  private final int numberOfIssues;
  private final String sqlHeader;
  private final boolean hasFunction;

  public SQLValidationTest2(String query, boolean isResultOk, boolean isParseError, int numberOfIssues, String sqlHeader,
      boolean hasFunction) {
    this.query = query;
    this.isResultOk = isResultOk;
    this.parseError = isParseError;
    this.numberOfIssues = numberOfIssues;
    this.sqlHeader = sqlHeader;
    this.hasFunction = hasFunction;
  }

  @Test
  public void testValidInvalidQueries() {
    DownloadsQueryRuleBase ruleBase = DownloadsQueryRuleBase.create();
    QueryContext queryContext = Hive.Parser.parse(query);
    ruleBase.fireAllRules(queryContext);
    assertEquals(isResultOk, !ruleBase.getRuleBaseContext().hasIssues());
    assertEquals(parseError, queryContext.hasParseIssues());
    assertEquals(numberOfIssues, ruleBase.getRuleBaseContext().issues().size());
    if (!ruleBase.getRuleBaseContext().hasIssues()) {
      QueryFragments fragments = queryContext.fragments(ruleBase).get();
      System.out.println("SQL:" + query);
      System.out.println("FROM:" + fragments.getFrom());
      System.out.println("FIELDS:" + fragments.getFields());
      System.out.println("WHERE:" + fragments.getWhere());
      System.out.println("FUNCTION:" + fragments.hasFunctionsOnSqlFields());
      System.out.println("EXPECTED:"+ sqlHeader+" ACTUAL:"+String.join(COMMA, fragments.getFields()).trim());
      assertEquals(hasFunction, fragments.hasFunctionsOnSqlFields());
      assertEquals(sqlHeader, String.join(COMMA, fragments.getFields()).trim());
    }
  }
}
