package org.gbif.occurrence.download.util;

import org.gbif.api.model.occurrence.SqlDownloadFunction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlValidationTest {

  @Test
  public void testAllFunctionsMapped() {
    assertEquals(SqlDownloadFunction.values().length, SqlValidation.additionalSqlOperators().size());
  }
}
