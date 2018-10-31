package org.gbif.occurrence.ws.provider.hive;

import java.io.IOException;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class SQLDescribeAndExplainTest {
  private static final ObjectMapper mapper = new ObjectMapper();
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();
  
  @Test
  public void testLegalExplain() {
    String result = HiveSQL.Execute.explain("SELECT * FROM occurrence_hdfs");
    Assert.assertTrue(!result.isEmpty());
  }

  @Test
  public void testIllLegalExplain() {
    expectedEx.expect(RuntimeException.class);
    String result = HiveSQL.Execute.explain("SELECT * FROM occurre");
    System.out.println(result);
  }
  
  @Test
  public void testValidDescribe() throws IOException  {
    List<DescribeResult> result = HiveSQL.Execute.describe("occurrence_hdfs");
    Assert.assertTrue(!result.isEmpty());
    System.out.println(mapper.writeValueAsString(result));
  }
  
  @Test
  public void testInValidDescribe()  {
    expectedEx.expect(RuntimeException.class);
    HiveSQL.Execute.describe("occurrence");
  }
}
