package org.gbif.occurrence.download.util;

import org.gbif.api.model.occurrence.SqlDownloadFunction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlValidationTest {

  @Test
  public void testAllFunctionsMapped() {
    // FIXME: delete the - 1 when EUROSTAT_CELL_CODE is added
    assertEquals(SqlDownloadFunction.values().length - 1, SqlValidation.additionalSqlOperators().size());
  }
}
