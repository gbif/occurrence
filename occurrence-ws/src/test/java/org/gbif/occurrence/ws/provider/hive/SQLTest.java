package org.gbif.occurrence.ws.provider.hive;

import java.io.IOException;
import org.gbif.occurrence.ws.provider.hive.HiveSQL.Validate.Result;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.cloudera.org.codehaus.jackson.JsonGenerationException;
import com.cloudera.org.codehaus.jackson.map.JsonMappingException;


public class SQLTest {

  private static final String COMPILATION_ERROR = "COMPILATION ERROR";
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testLegal() {
    String result = new HiveSQL.Execute().explain("SELECT * FROM occurrence_hdfs");
    Assert.assertEquals(true, !result.isEmpty());
  }

  @Test
  public void testIllLegal() {
    expectedEx.expect(RuntimeException.class);
    String result = new HiveSQL.Execute().explain("SELECT * FROM occurre");
    System.out.println(result);
  }

  @Test
  public void testValid1() {
    Result result = new HiveSQL.Validate().apply("SELECT gbifid, datasetkey, license from occurrence");
    Assert.assertEquals(true, result.isOk());
    Assert.assertNotEquals(COMPILATION_ERROR, result.explain());
    Assert.assertEquals(0, result.issues().size());
  }
  
  @Test
  public void testValid2() {
    Result result = new HiveSQL.Validate().apply("SELECT * from occurrence");
    Assert.assertEquals(true, result.isOk());
    Assert.assertNotEquals(COMPILATION_ERROR, result.explain());
    Assert.assertEquals(0, result.issues().size());
  }
  
  @Test
  public void testValid3() {
    Result result = new HiveSQL.Validate().apply("SELECT * from occurrence where countrycode='US'");
    Assert.assertEquals(true, result.isOk());
    Assert.assertNotEquals(COMPILATION_ERROR, result.explain());
    Assert.assertEquals(0, result.issues().size());
  }
  
  @Test
  public void testValid4() {
    Result result = new HiveSQL.Validate().apply("SELECT COUNT(datasetkey), countrycode ,datasetkey ,license\n FROM occurrence GROUP BY countrycode, license, datasetkey");
    Assert.assertEquals(true, result.isOk());
    Assert.assertNotEquals(COMPILATION_ERROR, result.explain());
    Assert.assertEquals(0, result.issues().size());
  }
  
  @Test
  public void testValid5() {
    Result result = new HiveSQL.Validate().apply("SELECT COUNT(datasetkey), countrycode ,datasetkey, license\n FROM occurrence GROUP BY countrycode, license, datasetkey HAVING count(datasetkey) > 5");
    Assert.assertEquals(true, result.isOk());
    Assert.assertNotEquals(COMPILATION_ERROR, result.explain());
    Assert.assertEquals(0, result.issues().size());
  }

  @Test
  public void testInValid1() {
    Result result = new HiveSQL.Validate().apply("SELECT col\n" + "FROM (\n" + "  SELECT a+b AS col\n" + "  FROM t1\n" + ") t2");
    Assert.assertEquals(false, result.isOk());
    Assert.assertEquals(COMPILATION_ERROR, result.explain());
    Assert.assertEquals(5, result.issues().size());
  }

  @Test
  public void testInvalid2() {
    Result result = new HiveSQL.Validate().apply("SELECT a.* FROM a JOIN b ON (a.id = b.id)");
    Assert.assertEquals(false, result.isOk());
    Assert.assertEquals(COMPILATION_ERROR, result.explain());
    Assert.assertEquals(4, result.issues().size());
  }

  @Test
  public void testInvalid3() throws JsonGenerationException, JsonMappingException, IOException {
    Result result = new HiveSQL.Validate().apply("SELECT key FROM (SELECT key FROM src ORDER BY key LIMIT 10)\n" + "    UNION\n"
        + "    SELECT key FROM (SELECT key FROM src1 ORDER BY key LIMIT 10)");
    Assert.assertEquals(false, result.isOk());
    Assert.assertEquals(COMPILATION_ERROR, result.explain());
    Assert.assertEquals(1, result.issues().size());
  }

  @Test
  public void testInvalid4()  {
    String result = new HiveSQL.Execute().describe("occurrence_hdfs");
    Assert.assertTrue(!result.isEmpty());
  }

}
