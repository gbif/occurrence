package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.DwcTerm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EventTableDefinitionTest {

  @Test
  public void eventTableDefinitionTest() {
    OccurrenceHDFSTableDefinition.definition().forEach(f -> System.out.println(f.getColumnName()));
  }

  @Test
  public void newTermsTest() {
    Assertions.assertTrue(
        EventHDFSTableDefinition.definition().stream()
            .anyMatch(t -> t.getTerm().equals(DwcTerm.fundingAttribution)));

    Assertions.assertTrue(
      OccurrenceHDFSTableDefinition.definition().stream()
        .noneMatch(t -> t.getTerm().equals(DwcTerm.fundingAttribution)));
  }
}
