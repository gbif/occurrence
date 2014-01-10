package org.gbif.occurrence.common.download;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.common.download.HiveFieldUtil;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HiveFieldUtilTest {

  @Test
  public void testGetHiveField() throws Exception {
    assertEquals("id", HiveFieldUtil.getHiveField(FieldName.ID));
    assertEquals("verbatim_basis_of_record", HiveFieldUtil.getHiveField(FieldName.BASIS_OF_RECORD));
    assertEquals("verbatim_class", HiveFieldUtil.getHiveField(FieldName.CLASS));
    assertEquals("verbatim_order", HiveFieldUtil.getHiveField(FieldName.ORDER));
    assertEquals("class_", HiveFieldUtil.getHiveField(FieldName.I_CLASS));
    assertEquals("order_", HiveFieldUtil.getHiveField(FieldName.I_ORDER));
    assertEquals("taxon_id", HiveFieldUtil.getHiveField(FieldName.I_NUB_ID));
    assertEquals("mod360_cell_id", HiveFieldUtil.getHiveField(FieldName.I_MOD360_CELL_ID));
    assertEquals("unit_qualifier", HiveFieldUtil.getHiveField(FieldName.UNIT_QUALIFIER));
    assertEquals("created", HiveFieldUtil.getHiveField(FieldName.CREATED));
    assertEquals("geospatial_issue", HiveFieldUtil.getHiveField(FieldName.I_GEOSPATIAL_ISSUE));
  }

  @Test
  public void testGetSimpleTermName() throws Exception {
    assertEquals("occurrenceID", HiveFieldUtil.getTerm(FieldName.ID).simpleName());
    assertEquals("verbatimBasisOfRecord", HiveFieldUtil.getTerm(FieldName.BASIS_OF_RECORD).simpleName());
    assertEquals("verbatimClass", HiveFieldUtil.getTerm(FieldName.CLASS).simpleName());
    assertEquals("verbatimOrder", HiveFieldUtil.getTerm(FieldName.ORDER).simpleName());
    assertEquals("class", HiveFieldUtil.getTerm(FieldName.I_CLASS).simpleName());
    assertEquals("order", HiveFieldUtil.getTerm(FieldName.I_ORDER).simpleName());
    assertEquals("taxonID", HiveFieldUtil.getTerm(FieldName.I_NUB_ID).simpleName());
    assertEquals("unitQualifier", HiveFieldUtil.getTerm(FieldName.UNIT_QUALIFIER).simpleName());
    assertEquals("created", HiveFieldUtil.getTerm(FieldName.CREATED).simpleName());
  }

  @Test
  public void testGetTermName() throws Exception {
    assertEquals(DwcTerm.occurrenceID, HiveFieldUtil.getTerm(FieldName.ID));
    assertEquals(DwcTerm.basisOfRecord, HiveFieldUtil.getTerm(FieldName.I_BASIS_OF_RECORD));
    assertEquals(GbifTerm.verbatimBasisOfRecord, HiveFieldUtil.getTerm(FieldName.BASIS_OF_RECORD));
    assertEquals(GbifTerm.verbatimClass, HiveFieldUtil.getTerm(FieldName.CLASS));
    assertEquals(GbifTerm.verbatimOrder, HiveFieldUtil.getTerm(FieldName.ORDER));
    assertEquals(DwcTerm.class_, HiveFieldUtil.getTerm(FieldName.I_CLASS));
    assertEquals(DwcTerm.order, HiveFieldUtil.getTerm(FieldName.I_ORDER));
    assertEquals(DwcTerm.taxonID, HiveFieldUtil.getTerm(FieldName.I_NUB_ID));
    assertEquals(GbifTerm.unitQualifier, HiveFieldUtil.getTerm(FieldName.UNIT_QUALIFIER));
    assertEquals(GbifTerm.created, HiveFieldUtil.getTerm(FieldName.CREATED));
  }
}
