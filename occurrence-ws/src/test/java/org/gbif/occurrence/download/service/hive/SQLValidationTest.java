package org.gbif.occurrence.download.service.hive;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.gbif.common.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.gbif.occurrence.download.service.hive.HiveSQL.Validate;
import org.gbif.occurrence.download.service.hive.validation.DownloadsQueryRuleBase;
import org.gbif.occurrence.download.service.hive.validation.HavingClauseNotSupportedRule;
import org.gbif.occurrence.download.service.hive.validation.Hive;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryFragments;
import org.gbif.occurrence.download.service.hive.validation.OnlyOneSelectAllowedRule;
import org.gbif.occurrence.download.service.hive.validation.OnlyPureSelectQueriesAllowedRule;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation.Rule;
import org.gbif.occurrence.download.service.hive.validation.StarForFieldsNotAllowedRule;
import org.gbif.occurrence.download.service.hive.validation.TableNameShouldBeOccurrenceRule;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SQLValidationTest {

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

  public SQLValidationTest(String query, boolean isResultOk, boolean isParseError, int numberOfIssues, String sqlHeader,
      boolean hasFunction) {
    this.query = query;
    this.isResultOk = isResultOk;
    this.parseError = isParseError;
    this.numberOfIssues = numberOfIssues;
    this.sqlHeader = sqlHeader;
    this.hasFunction = hasFunction;
  }
  
  static class QueryTestValidate extends Validate{
    //avoiding SQLShouldBeExecutableRule
    private static final List<Rule> RULES = Arrays.asList(new OnlyOneSelectAllowedRule(), new StarForFieldsNotAllowedRule(),
        new OnlyPureSelectQueriesAllowedRule(), new TableNameShouldBeOccurrenceRule(), new HavingClauseNotSupportedRule());

    
    @Override
    public Result apply(String sql) {
      
      DownloadsQueryRuleBase ruleBase = DownloadsQueryRuleBase.create(RULES);
      QueryContext queryContext = Hive.Parser.parse(sql);
      if (queryContext.hasParseIssues())
        return parseFailedResultTemplate.apply(queryContext);

      ruleBase.fireAllRules(queryContext);
      
      if (ruleBase.context().hasIssues())
        return ruleFailedResultTemplate.apply(queryContext, ruleBase.context());

      queryContext.computeFragmentsAndTranslateSQL(ruleBase);
      QueryFragments fragments = queryContext.fragments();
      List<String> explain = queryContext.explainQuery();
      String sqlHeader = String.join(TAB, fragments.getFields());
      String translatedQuery = queryContext.translatedSQL();
      return new Result(queryContext.sql(), translatedQuery, ruleBase.context().issues(), explain, sqlHeader, queryContext,
          ruleBase.context().issues().isEmpty());
    }
  }
  
  @Test
  public void testValidInvalidQueries() throws JsonProcessingException {
    HiveSQL.Validate.Result result = new QueryTestValidate().apply(query); 
    Assert.assertEquals(isResultOk, result.isOk());
    Assert.assertEquals(parseError, result.issues().contains(Issue.PARSE_FAILED));
    Assert.assertEquals(numberOfIssues, result.issues().size());  
    if(numberOfIssues == 0) {
      Assert.assertEquals(sqlHeader, String.join(COMMA, result.queryContext().fragments().getFields()).trim()); 
      Assert.assertEquals(hasFunction, result.queryContext().fragments().hasFunctionsOnSqlFields());
      Assert.assertTrue(result.transSql().contains("occurrence_hdfs"));
    }
  }
}
