package org.gbif.occurrence.download.service.hive;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.gbif.occurrence.download.service.hive.validation2.HavingClauseNotSupportedRule;
import org.gbif.occurrence.download.service.hive.validation2.Hive;
import org.gbif.occurrence.download.service.hive.validation2.OnlyOneSelectAllowedRule;
import org.gbif.occurrence.download.service.hive.validation2.OnlyPureSelectQueriesAllowedRule;
import org.gbif.occurrence.download.service.hive.validation2.Rule;
import org.gbif.occurrence.download.service.hive.validation2.RuleBase;
import org.gbif.occurrence.download.service.hive.validation2.StarForFieldsNotAllowedRule;
import org.gbif.occurrence.download.service.hive.validation2.TableNameShouldBeOccurrenceRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class QueryValidator2 {

  static List<Rule> rules = Arrays.asList(new OnlyOneSelectAllowedRule(), new StarForFieldsNotAllowedRule(), new OnlyPureSelectQueriesAllowedRule(),new TableNameShouldBeOccurrenceRule(), new HavingClauseNotSupportedRule());
  
  @Parameters
  public static Collection<Object[]> inputs() {
    return Arrays.asList( new Object[][] {
      {"SELECT gbifid, datasetkey, license from occurrence", true}, 
      {"SELECT gbifid, countrycode, datasetkey, license from occurrence", true},
      {"SELECT gbifid, countrycode, datasetkey, license from occurrence where countrycode='US'", true},
      {"SELECT gbifid, countrycode, datasetkey, license, month, year FROM occurrence WHERE month=3 AND year = 2018", true},
      {"SELECT COUNT(datasetkey), countrycode ,datasetkey ,license FROM occurrence GROUP BY countrycode, license, datasetkey", true},
      {"SELECT COUNT(datasetkey), countrycode ,datasetkey, license FROM occurrence GROUP BY countrycode, license, datasetkey HAVING count(datasetkey) > 5", false},
      {"SELECT key FROM (SELECT key FROM src ORDER BY key LIMIT 10) UNION SELECT key FROM (SELECT key FROM src1 ORDER BY key LIMIT 10)", false},
      {"SELECT max(year) FROM occurrence GROUP BY year HAVING year > 2000", false},
      {"SELECT COUNT(countrycode) CODE FROM occurrence_hdfs WHERE month IN (Select month from occurrence)  and year=2004 HAVING countrycode='CO'",false},
      {"SELECT * from occurrence",false} 
  });
  } 
  
  private final String query;
  private final boolean isResultOk;
  
  
  public QueryValidator2(String query, boolean isResultOk) {
    this.query = query;
    this.isResultOk = isResultOk;
  }
  
  @Test
  public void testValidInvalidQueries() {
   RuleBase ruleBase =RuleBase.create(rules);
    ruleBase.fireAllRules(Hive.Parser.parse(query));
    assertEquals(isResultOk, ruleBase.getRuleBaseContext().issues().size()==0);
    System.out.println(ruleBase.getRuleBaseContext().issues());
  }

  
}
