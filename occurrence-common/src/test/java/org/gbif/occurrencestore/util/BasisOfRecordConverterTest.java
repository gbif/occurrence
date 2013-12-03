package org.gbif.occurrencestore.util;

import org.gbif.api.vocabulary.BasisOfRecord;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BasisOfRecordConverterTest {

  private BasisOfRecordConverter converter = new BasisOfRecordConverter();

  @Test
  public void testToEnum() throws Exception {
    assertEquals(BasisOfRecord.UNKNOWN, converter.toEnum(0));
    assertEquals(BasisOfRecord.UNKNOWN, converter.toEnum(null));
    assertEquals(BasisOfRecord.UNKNOWN, converter.toEnum(1999));
    assertEquals(BasisOfRecord.FOSSIL_SPECIMEN, converter.toEnum(5));
    assertEquals(BasisOfRecord.OBSERVATION, converter.toEnum(1));
  }

  @Test
  public void testFromEnum() throws Exception {
    assertEquals((Integer) 0, converter.fromEnum(BasisOfRecord.UNKNOWN));
    assertEquals((Integer) 5, converter.fromEnum(BasisOfRecord.FOSSIL_SPECIMEN));
    assertEquals((Integer) 1, converter.fromEnum(BasisOfRecord.OBSERVATION));
    assertNull(converter.fromEnum(null));
  }
}
