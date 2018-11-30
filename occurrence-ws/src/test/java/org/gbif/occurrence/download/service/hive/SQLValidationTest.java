package org.gbif.occurrence.download.service.hive;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.gbif.occurrence.download.service.hive.HiveSQL.Validate.Result;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation.QueryContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import com.google.common.collect.Lists;

/**
 * Testing validation queries.
 *
 */
@RunWith(Parameterized.class)
public class SQLValidationTest {

  private static final String COMPILATION_ERROR = "COMPILATION ERROR";

  /**
   * 
   * Custom implementation to validate Rules on SQL query.
   *
   */
  static class ValidateTest extends HiveSQL.Validate{
    @Override
    public HiveSQL.Validate.Result apply(String sql) {
      List<Issue> issues = Lists.newArrayList();

      QueryContext context = QueryContext.from(sql).onParseFail(issues::add);
      if (context.hasParseIssue())
        return new Result(context.sql(), context.translatedQuery(), issues, Arrays.asList(COMPILATION_ERROR), "", context, issues.isEmpty());

      RULES.forEach(rule -> rule.apply(context).onViolation(issues::add));      
      String sqlHeader = String.join(TAB, context.selectFieldNames());
      return new Result(context.sql(), context.translatedQuery(), issues, Arrays.asList(""), sqlHeader, context, issues.isEmpty());
    }
  }

  @Parameters
  public static Collection<Object[]> inputs() {
    return Arrays.asList( new Object[][] {
      {"SELECT `gbifid`, `datasetkey`, `license` from occurrence", true, false, 0, "gbifid,datasetkey,license", false}, 
      {"SELECT `gbifid`, `countrycode`, `datasetkey`, `license` from occurrence", true, false, 0, "gbifid,countrycode,datasetkey,license", false},
      {"SELECT `gbifid`, `countrycode`, `datasetkey`, `license` from `occurrence` where `countrycode`='US'", true, false, 0, "gbifid,countrycode,datasetkey,license", false},
      {"SELECT `gbifid`, `countrycode`, `datasetkey`, `license`, `month`, `year` FROM `occurrence` WHERE `month`=3 AND `year` = 2018", true, false, 0, "gbifid,countrycode,datasetkey,license,month,year", false},
      {"SELECT COUNT(`datasetkey`), `countrycode` ,`datasetkey` ,`license` FROM `occurrence` GROUP BY `countrycode`, `license`, `datasetkey`", true, false, 0, "COUNT(`datasetkey`),countrycode,datasetkey,license", true},
      {"SELECT COUNT(`datasetkey`), `countrycode` ,`datasetkey`, `license` FROM `occurrence` GROUP BY `countrycode`, `license`, `datasetkey` HAVING count(`datasetkey`) > 5", false, false, 1, "COUNT(`datasetkey`),countrycode,datasetkey,license", true},
      {"SELECT key FROM (SELECT key FROM src ORDER BY key LIMIT 10) UNION SELECT key FROM (SELECT key FROM src1 ORDER BY key LIMIT 10)", false, true ,1, "", false}
  });
  } 
  
  private final String query;
  private final boolean isResultOk;
  private final boolean isCompilationError;
  private final int numberOfIssues;
  private final String sqlHeader;
  private final boolean hasFunction;
  
  public SQLValidationTest(String query, boolean isResultOk, boolean isCompilationError, int numberOfIssues, String sqlHeader, boolean hasFunction) {
    this.query = query;
    this.isResultOk = isResultOk;
    this.isCompilationError = isCompilationError;
    this.numberOfIssues = numberOfIssues;
    this.sqlHeader = sqlHeader;
    this.hasFunction = hasFunction;
  }
  
  @Test
  public void testValidInvalidQueries() {
    Result result = new ValidateTest().apply(query);   
    Assert.assertEquals(isResultOk, result.isOk());
    Assert.assertEquals(isCompilationError, result.explain().equals(Arrays.asList(COMPILATION_ERROR)));
    Assert.assertEquals(numberOfIssues, result.issues().size());  
    Assert.assertEquals(sqlHeader, result.sqlHeader().replaceAll("\t", ",").trim()); 
    Assert.assertEquals(hasFunction, result.queryContext().hasFunctionsInSelectFields());
  }

}
