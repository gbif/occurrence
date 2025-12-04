package org.gbif.occurrence.download.hive;

import org.junit.jupiter.api.Test;

public class EventTableDefinitionTest {

  @Test
  public void eventTableDefinitionTest() {
    OccurrenceHDFSTableDefinition.definition().forEach(f -> System.out.println(f.getColumnName()));
  }
}
