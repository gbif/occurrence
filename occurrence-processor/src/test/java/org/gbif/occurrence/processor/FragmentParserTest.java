package org.gbif.occurrence.processor;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.persistence.api.Fragment;
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
    assertEquals("BGBM", got.getField(DwcTerm.institutionCode));
    assertEquals("AlgaTerra", got.getField(DwcTerm.collectionCode));
    assertEquals("5834", got.getField(DwcTerm.catalogNumber));
    assertEquals(datasetKey, got.getDatasetKey());
    assertNull(got.getField(GbifTerm.unitQualifier));

    assertEquals(1, got.getKey().intValue());
    assertEquals("Tetraedron caudatum (Corda) Hansg.", got.getField(DwcTerm.scientificName));
    assertEquals("52.423798", got.getField(DwcTerm.decimalLatitude));
    assertEquals("13.191434", got.getField(DwcTerm.decimalLongitude));
    assertEquals("50", got.getField(DwcTerm.coordinatePrecision));
    assertEquals("400", got.getField(DwcTerm.minimumElevationInMeters));
    assertEquals("500", got.getField(DwcTerm.maximumElevationInMeters));
    assertEquals("DE", got.getField(DwcTerm.country));
    assertEquals("Kusber, W.-H.", got.getField(DwcTerm.recordedBy));
    assertEquals("Nikolassee, Berlin", got.getField(DwcTerm.locality));
    assertEquals("1987-04-13T00:00:00", got.getField(DwcTerm.eventDate));
    assertEquals("HumanObservation", got.getField(DwcTerm.basisOfRecord));
    assertEquals("Kusber, W.-H.", got.getField(DwcTerm.identifiedBy));

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

    assertEquals("UGENT", got.getField(DwcTerm.institutionCode));
    assertEquals("vertebrata", got.getField(DwcTerm.collectionCode));
    assertEquals("50058", got.getField(DwcTerm.catalogNumber));
    assertEquals(datasetKey, got.getDatasetKey());
    assertNull(got.getField(GbifTerm.unitQualifier));

    assertEquals("Alouatta villosa Gray, 1845", got.getField(DwcTerm.scientificName));
    assertEquals("Gray, 1845", got.getField(DwcTerm.scientificNameAuthorship));
    assertEquals("Animalia", got.getField(DwcTerm.kingdom));
    assertEquals("Chordata", got.getField(DwcTerm.phylum));
    assertEquals("Mammalia", got.getField(DwcTerm.class_));
    assertEquals("Primates", got.getField(DwcTerm.order));
    assertEquals("Atelidae", got.getField(DwcTerm.family));
    assertEquals("Alouatta", got.getField(DwcTerm.genus));
    assertEquals("villosa", got.getField(DwcTerm.specificEpithet));
    assertEquals("25", got.getField(DwcTerm.coordinatePrecision));
    assertEquals("200", got.getField(DwcTerm.minimumElevationInMeters));
    assertEquals("400", got.getField(DwcTerm.maximumElevationInMeters));
    assertEquals("PreservedSpecimen", got.getField(DwcTerm.basisOfRecord));
  }

}
