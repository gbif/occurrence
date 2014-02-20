package org.gbif.occurrence.ws.client.mock;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.dwc.terms.DwcTerm;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OccurrencePersistenceMockServiceTest {

  @Test
  public void testGenerator() {
    OccurrencePersistenceMockService srv = new OccurrencePersistenceMockService();
    Occurrence occ = srv.get(112);
    assertEquals((Integer) 112, occ.getKey());
    assertEquals(OccurrencePersistenceMockService.DATASETS.get(2), occ.getDatasetKey());
    assertEquals("cat-112", occ.getVerbatimField(DwcTerm.catalogNumber));
    assertEquals("Chromista", occ.getKingdom());
    assertEquals((Integer) 4, occ.getKingdomKey());
    assertEquals(BasisOfRecord.PRESERVED_SPECIMEN, occ.getBasisOfRecord());
    assertEquals((Integer) 1912, occ.getYear());
    assertEquals((Integer) 5, occ.getMonth());
    assertEquals( (Double) 112d, occ.getElevation());
  }
}
