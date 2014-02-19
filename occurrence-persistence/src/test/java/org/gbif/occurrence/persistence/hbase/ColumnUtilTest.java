package org.gbif.occurrence.persistence.hbase;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.persistence.api.InternalTerm;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class ColumnUtilTest {


  @Test
  public void testGetColumn() throws Exception {
    assertEquals("scientificName", ColumnUtil.getColumn(DwcTerm.scientificName));
    assertEquals("countryCode", ColumnUtil.getColumn(DwcTerm.countryCode));
    assertEquals("v_catalogNumber", ColumnUtil.getColumn(DwcTerm.catalogNumber));
    assertEquals("class", ColumnUtil.getColumn(DwcTerm.class_));
    assertEquals("order", ColumnUtil.getColumn(DwcTerm.order));
    assertEquals("kingdomKey", ColumnUtil.getColumn(GbifTerm.kingdomKey));
    //TODO: is this correct ???
    assertEquals("taxonKey", ColumnUtil.getColumn(GbifTerm.taxonKey));
    assertEquals("v_occurrenceID", ColumnUtil.getColumn(DwcTerm.occurrenceID));
    assertEquals("v_taxonID", ColumnUtil.getColumn(DwcTerm.taxonID));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetVerbatimColumnIllegal() {
    ColumnUtil.getVerbatimColumn(InternalTerm.crawlId);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetColumnIllegal3() {
    ColumnUtil.getColumn(DwcTerm.country);
  }

  public void testGetVerbatimColumn() throws Exception {

  }

  public void testGetTermFromVerbatimColumn() throws Exception {

  }

  public void testGetColumn1() throws Exception {

  }
}
