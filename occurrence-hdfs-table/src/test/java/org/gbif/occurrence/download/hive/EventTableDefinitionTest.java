package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.ObisTerm;
import org.gbif.dwc.terms.Term;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EventTableDefinitionTest {

  @Test
  public void eventTableDefinitionTest() {
    EventHDFSTableDefinition.definition().forEach(f -> System.out.println(f.getColumnName()));
  }

  @Test
  public void newTermsTest() {
    assertContainsTerm(DwcTerm.fundingAttribution);
    assertContainsTerm(DwcTerm.projectID);
    assertContainsTerm(DwcTerm.measurementType);
    assertContainsTerm(ObisTerm.measurementTypeID);
  }

  private void assertContainsTerm(Term term) {
    Assertions.assertTrue(
        EventHDFSTableDefinition.definition().stream()
            .anyMatch(t -> t.getColumnName().equals(term.simpleName().toLowerCase())));
  }
}
