package org.gbif.occurrence.download.service.hive;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class SQLDescribeAndExplainTest {

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();
  
  @Test
  public void testLegalExplain() {
    String result = new HiveSQL.Execute().explain("SELECT * FROM occurrence_hdfs");
    Assert.assertTrue(!result.isEmpty());
  }

  @Test
  public void testIllLegalExplain() {
    expectedEx.expect(RuntimeException.class);
    String result = new HiveSQL.Execute().explain("SELECT * FROM occurre");
    System.out.println(result);
  }
  
  @Test
  public void testValidDescribe()  {
    String result = new HiveSQL.Execute().describe("occurrence_hdfs");
    Assert.assertTrue(!result.isEmpty());
  }
  
  @Test
  public void testInValidDescribe()  {
    expectedEx.expect(RuntimeException.class);
    new HiveSQL.Execute().describe("occurrence");
  }
}
