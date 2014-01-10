package org.gbif.occurrence.processor;

import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.VerbatimOccurrence;
import org.gbif.occurrence.processor.parsing.FragmentParser;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import com.google.common.io.Resources;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.Charsets;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class FragmentParserTest {

  private static String abcd206Single;
  private static String dwc14;

  @BeforeClass
  public static void preClass() throws IOException {
    abcd206Single = Resources.toString(Resources.getResource("abcd206_single.xml"), Charsets.UTF_8);
    dwc14 = Resources.toString(Resources.getResource("dwc14.xml"), Charsets.UTF_8);
  }

  @Test
  public void testAbcd206() {
    OccurrenceSchemaType schema = OccurrenceSchemaType.ABCD_2_0_6;
    UUID datasetKey = UUID.randomUUID();
    Fragment frag = new Fragment(datasetKey, abcd206Single.getBytes(), DigestUtils.md5(abcd206Single.getBytes()),
      Fragment.FragmentType.XML, EndpointType.BIOCASE, new Date(), 1, schema, null, null);
    frag.setKey(1);

    VerbatimOccurrence got = FragmentParser.parse(frag);
    assertNotNull(got);
    assertEquals("BGBM", got.getInstitutionCode());
    assertEquals("AlgaTerra", got.getCollectionCode());
    assertEquals("5834", got.getCatalogNumber());
    assertEquals(datasetKey, got.getDatasetKey());
    assertNull(got.getUnitQualifier());

    assertEquals(1, got.getKey().intValue());
    assertEquals("Tetraedron caudatum (Corda) Hansg.", got.getScientificName());
    assertEquals("52.423798", got.getLatitude());
    assertEquals("13.191434", got.getLongitude());
    assertEquals("50", got.getLatLongPrecision());
    assertEquals("400", got.getMinAltitude());
    assertEquals("500", got.getMaxAltitude());
    assertEquals("DE", got.getCountry());
    assertEquals("Kusber, W.-H.", got.getCollectorName());
    assertEquals("Nikolassee, Berlin", got.getLocality());
    assertEquals("1987-04-13T00:00:00", got.getOccurrenceDate());
    assertEquals("HumanObservation", got.getBasisOfRecord());
    assertEquals("Kusber, W.-H.", got.getIdentifierName());
  }

  @Test
  public void testDwc14() {
    OccurrenceSchemaType schema = OccurrenceSchemaType.DWC_1_4;
    UUID datasetKey = UUID.randomUUID();
    Fragment frag =
      new Fragment(datasetKey, dwc14.getBytes(), DigestUtils.md5(dwc14.getBytes()), Fragment.FragmentType.XML,
        EndpointType.DIGIR, new Date(), 1, schema, null, null);

    VerbatimOccurrence got = FragmentParser.parse(frag);
    assertNotNull(got);
    assertEquals("UGENT", got.getInstitutionCode());
    assertEquals("vertebrata", got.getCollectionCode());
    assertEquals("50058", got.getCatalogNumber());
    assertEquals(datasetKey, got.getDatasetKey());
    assertNull(got.getUnitQualifier());

    assertEquals("Alouatta villosa Gray, 1845", got.getScientificName());
    assertEquals("Gray, 1845", got.getAuthor());
    assertEquals("Animalia", got.getKingdom());
    assertEquals("Chordata", got.getPhylum());
    assertEquals("Mammalia", got.getKlass());
    assertEquals("Primates", got.getOrder());
    assertEquals("Atelidae", got.getFamily());
    assertEquals("Alouatta", got.getGenus());
    assertEquals("villosa", got.getSpecies());
    assertEquals("25", got.getLatLongPrecision());
    assertEquals("200", got.getMinAltitude());
    assertEquals("400", got.getMaxAltitude());
    assertEquals("PreservedSpecimen", got.getBasisOfRecord());
  }

  // TODO identifier records
}
