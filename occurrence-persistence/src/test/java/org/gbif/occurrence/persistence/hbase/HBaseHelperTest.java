package org.gbif.occurrence.persistence.hbase;

import org.gbif.api.vocabulary.BasisOfRecord;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HBaseHelperTest {

  @Test
  public void testEnums() {
    BasisOfRecord bor = HBaseHelper.nullSafeEnum(BasisOfRecord.class, "LIVING_SPECIMEN");
    assertNotNull(bor);
    assertEquals(BasisOfRecord.LIVING_SPECIMEN, bor);

    bor = HBaseHelper.nullSafeEnum(BasisOfRecord.class, "LIVING_SPECIMEN ");
    assertNotNull(bor);
    assertEquals(BasisOfRecord.LIVING_SPECIMEN, bor);
  }
}
