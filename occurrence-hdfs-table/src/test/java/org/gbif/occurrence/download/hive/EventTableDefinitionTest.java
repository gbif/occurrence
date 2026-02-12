package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EventTableDefinitionTest {

  @Test
  public void eventTableDefinitionTest() {
    EventHDFSTableDefinition.definition().forEach(f -> System.out.println(f.getColumnName()));
  }

  @Test
  public void newTermsTest() {
    Assertions.assertTrue(
        EventHDFSTableDefinition.definition().stream()
            .anyMatch(
                t ->
                    t.getColumnName()
                        .equals(DwcTerm.fundingAttribution.simpleName().toLowerCase())));

    Assertions.assertTrue(
        EventHDFSTableDefinition.definition().stream()
            .anyMatch(t -> t.getColumnName().equals(DwcTerm.projectID.simpleName().toLowerCase())));

    Assertions.assertTrue(
        OccurrenceHDFSTableDefinition.definition().stream()
            .noneMatch(
                t ->
                    t.getColumnName()
                        .equals(DwcTerm.fundingAttribution.simpleName().toLowerCase())));

    Assertions.assertTrue(
        OccurrenceHDFSTableDefinition.definition().stream()
            .anyMatch(
                t ->
                    t.getColumnName()
                        .equals("v_" + DwcTerm.fundingAttribution.simpleName().toLowerCase())));

    Assertions.assertTrue(
        OccurrenceHDFSTableDefinition.definition().stream()
            .anyMatch(
                t -> t.getColumnName().equals(GbifTerm.projectId.simpleName().toLowerCase())));
  }
}
