package org.gbif.occurrence.download.hive;

import org.junit.jupiter.api.Test;

public class OccurrenceAvroHdfsTableDefinitionTest {

  @Test
  public void avroSchemaTest() {
    System.out.println(OccurrenceAvroHdfsTableDefinition.avroDefinition());
  }

  @Test
  public void hiveSchemaTest() {
    System.out.println(OccurrenceHDFSTableDefinition.definition());
  }
}
