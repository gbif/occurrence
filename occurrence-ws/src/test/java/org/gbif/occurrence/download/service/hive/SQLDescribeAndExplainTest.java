package org.gbif.occurrence.download.service.hive;

import org.gbif.api.model.occurrence.sql.DescribeResult;
import org.gbif.occurrence.ws.OccurrenceWsListener;

import java.io.IOException;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test for legal and illegal explain and describe SQL commands.
 */
@Ignore
public class SQLDescribeAndExplainTest extends OccurrenceWsListener {

  private SqlDownloadService sqlDownloadService;

  public SQLDescribeAndExplainTest() throws IOException {
    super();
    sqlDownloadService = this.getInjector().getInstance(SqlDownloadService.class);
  }

  private static final ObjectMapper mapper = new ObjectMapper();

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testLegalExplain() {
    List<String> result = sqlDownloadService.explain("SELECT * FROM occurrence_hdfs");
    Assert.assertTrue(!result.isEmpty());
    System.out.println(result);
  }

  @Test
  public void testIllLegalExplain() {
    expectedEx.expect(RuntimeException.class);
    List<String> result = sqlDownloadService.explain("SELECT * FROM occurre");
    System.out.println(result);
  }

  @Test
  public void testValidDescribe() throws IOException {
    List<DescribeResult> result = sqlDownloadService.describe("occurrence_hdfs");
    Assert.assertTrue(!result.isEmpty());
    System.out.println(mapper.writeValueAsString(result));
  }

  @Test
  public void testInValidDescribe() {
    expectedEx.expect(RuntimeException.class);
    sqlDownloadService.describe("occurrence");
  }
}
