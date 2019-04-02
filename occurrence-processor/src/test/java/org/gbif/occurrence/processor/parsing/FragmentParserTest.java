package org.gbif.occurrence.processor.parsing;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.persistence.api.Fragment;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
    frag.setKey(1L);

    VerbatimOccurrence got = FragmentParser.parse(frag);
    assertNotNull(got);
    assertEquals("BGBM", got.getVerbatimField(DwcTerm.institutionCode));
    assertEquals("AlgaTerra", got.getVerbatimField(DwcTerm.collectionCode));
    assertEquals("5834", got.getVerbatimField(DwcTerm.catalogNumber));
    assertEquals(datasetKey, got.getDatasetKey());
    assertNull(got.getVerbatimField(GbifInternalTerm.unitQualifier));

    assertEquals(1, got.getKey().intValue());
    assertEquals("Tetraedron caudatum (Corda) Hansg.", got.getVerbatimField(DwcTerm.scientificName));
    assertEquals("52.123456", got.getVerbatimField(DwcTerm.decimalLatitude));
    assertEquals("13.123456", got.getVerbatimField(DwcTerm.decimalLongitude));
    assertEquals("50", got.getVerbatimField(DwcTerm.coordinateUncertaintyInMeters));
    assertEquals("400", got.getVerbatimField(DwcTerm.minimumElevationInMeters));
    assertEquals("500", got.getVerbatimField(DwcTerm.maximumElevationInMeters));
    assertEquals("DE", got.getVerbatimField(DwcTerm.country));
    assertEquals("Kusber, W.-H.", got.getVerbatimField(DwcTerm.recordedBy));
    assertEquals("Nikolassee, Berlin", got.getVerbatimField(DwcTerm.locality));
    assertEquals("1987-04-13T00:00:00", got.getVerbatimField(DwcTerm.eventDate));
    assertEquals("HumanObservation", got.getVerbatimField(DwcTerm.basisOfRecord));
    assertEquals("Kusber, W.-H.", got.getVerbatimField(DwcTerm.identifiedBy));
    assertEquals("Holotype", got.getVerbatimField(DwcTerm.typeStatus));
    assertEquals("Tetraedron caudatum (Corda) Hansg.", got.getVerbatimField(GbifTerm.typifiedName));

    assertNotNull(got.getExtensions().get(Extension.MULTIMEDIA));
    List<Map<Term,String>> mediaObjects = got.getExtensions().get(Extension.MULTIMEDIA);
    assertEquals(2, mediaObjects.size());
    Map<Term,String> medium = mediaObjects.get(0);
    assertEquals("http://www.tierstimmenarchiv.de/recordings/Ailuroedus_buccoides_V2010_04_short.mp3",
      medium.get(DcTerm.identifier));
    assertEquals("http://www.tierstimmenarchiv.de/webinterface/contents/showdetails.php?edit=-1&unique_id=TSA:Ailuroedus_buccoides_V_2010_4_1&autologin=true",
      medium.get(DcTerm.references));
    assertEquals("audio/mp3", medium.get(DcTerm.format));
    assertEquals("CC BY-NC-ND (Attribution for non commercial use only and without derivative)",
      medium.get(DcTerm.license));

  }

  @Test
  public void testDwc14() {
    OccurrenceSchemaType schema = OccurrenceSchemaType.DWC_1_4;
    UUID datasetKey = UUID.randomUUID();
    Fragment frag =
      new Fragment(datasetKey, dwc14.getBytes(), DigestUtils.md5(dwc14.getBytes()), Fragment.FragmentType.XML,
        EndpointType.DIGIR, new Date(), 1, schema, null, null);
    frag.setKey(123L);

    VerbatimOccurrence got = FragmentParser.parse(frag);
    assertNotNull(got);

    assertEquals("UGENT", got.getVerbatimField(DwcTerm.institutionCode));
    assertEquals("vertebrata", got.getVerbatimField(DwcTerm.collectionCode));
    assertEquals("50058", got.getVerbatimField(DwcTerm.catalogNumber));
    assertEquals(datasetKey, got.getDatasetKey());
    assertNull(got.getVerbatimField(GbifInternalTerm.unitQualifier));

    assertEquals("Alouatta villosa Gray, 1845", got.getVerbatimField(DwcTerm.scientificName));
    assertEquals("Gray, 1845", got.getVerbatimField(DwcTerm.scientificNameAuthorship));
    assertEquals("Animalia", got.getVerbatimField(DwcTerm.kingdom));
    assertEquals("Chordata", got.getVerbatimField(DwcTerm.phylum));
    assertEquals("Mammalia", got.getVerbatimField(DwcTerm.class_));
    assertEquals("Primates", got.getVerbatimField(DwcTerm.order));
    assertEquals("Atelidae", got.getVerbatimField(DwcTerm.family));
    assertEquals("Alouatta", got.getVerbatimField(DwcTerm.genus));
    assertEquals("villosa", got.getVerbatimField(DwcTerm.specificEpithet));
    assertEquals("25", got.getVerbatimField(DwcTerm.coordinateUncertaintyInMeters));
    assertEquals("200", got.getVerbatimField(DwcTerm.minimumElevationInMeters));
    assertEquals("400", got.getVerbatimField(DwcTerm.maximumElevationInMeters));
    assertEquals("PreservedSpecimen", got.getVerbatimField(DwcTerm.basisOfRecord));
    assertEquals("123", got.getVerbatimField(GbifTerm.gbifID));
    assertEquals("Holotype", got.getVerbatimField(DwcTerm.typeStatus));
  }

}
