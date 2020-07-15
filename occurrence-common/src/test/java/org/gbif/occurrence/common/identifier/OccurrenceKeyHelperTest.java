package org.gbif.occurrence.common.identifier;

import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.OccurrenceKeyHelper;
import org.gbif.occurrence.common.identifier.PublisherProvidedUniqueIdentifier;

import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OccurrenceKeyHelperTest {

  private static final UUID DATASET_KEY = UUID.randomUUID();
  private static final String IC = "BGBM";
  private static final String CC = "Vascular Plants";
  private static final String CN = "00234-asdfa-234as-asdf-cvb";
  private static final String UQ = "Abies alba";
  private static final String DWC = "98098098-234asd-asdfa-234df";

  private static final HolyTriplet TRIPLET = new HolyTriplet(DATASET_KEY, IC, CC, CN, UQ);
  private static final PublisherProvidedUniqueIdentifier PUB_PROVIDED = new PublisherProvidedUniqueIdentifier(DATASET_KEY, DWC);

  @Test
  public void testTripletKey() {
    String testKey = OccurrenceKeyHelper.buildKey(TRIPLET);
    Assertions.assertTrue(testKey.equals(DATASET_KEY.toString() + "|" + IC + "|" + CC + "|" + CN + "|" + UQ));
  }

  @Test
  public void testSingleDwcKey() {
    String testKey = OccurrenceKeyHelper.buildKey(PUB_PROVIDED);
    Assertions.assertTrue(testKey.equals(DATASET_KEY.toString() + "|" + DWC));
  }
}
