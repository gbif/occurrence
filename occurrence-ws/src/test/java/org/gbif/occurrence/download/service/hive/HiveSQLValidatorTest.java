package org.gbif.occurrence.download.service.hive;
import java.util.Arrays;
import java.util.Collection;
import org.gbif.occurrence.download.service.hive.validation2.UnionDDLJoinsValidator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class HiveSQLValidatorTest {
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] {
        {"SELECT * FROM occurrence_hdfs o JOIN multimedia m ON h.id = m.id", false},
        {"INSERT INTO occurrence_hdfs VALUES('1','2','3')", false},
        {"DELETE FROM occurrence WHERE id IS NULL", false}
      });
    }
  
    private final String query;
    private final boolean valid;
  
    public HiveSQLValidatorTest(String query, boolean valid) {
      this.query = query;
      this.valid = valid;
    }
  
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidQueries() {
      UnionDDLJoinsValidator hiveSQLValidator = new UnionDDLJoinsValidator();
      hiveSQLValidator.validateQuery(query);
      if (!valid) {
        Assert.fail("An invalid query has been accepted");
      }
    }
}
