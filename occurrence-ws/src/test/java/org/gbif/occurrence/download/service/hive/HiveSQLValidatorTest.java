package org.gbif.occurrence.download.service.hive;

import org.gbif.occurrence.download.service.hive.validation.UnionDDLJoinsValidator;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class HiveSQLValidatorTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{"SELECT * FROM occurrence_hdfs o JOIN multimedia m ON h.id = m.id", false},
      {"INSERT INTO occurrence_hdfs VALUES('1','2','3')", false}, {"DELETE FROM occurrence WHERE id IS NULL", false}});
  }

  private final String query;
  private final boolean valid;

  public HiveSQLValidatorTest(String query, boolean valid) {
    this.query = query;
    this.valid = valid;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidQueries() throws ParseException {
    ASTNode node = new ParseDriver().parse(query);
    UnionDDLJoinsValidator hiveSQLValidator = new UnionDDLJoinsValidator();
    hiveSQLValidator.validateNode(node);
    if (!valid) {
      Assert.fail("An invalid query has been accepted");
    }
  }
}
