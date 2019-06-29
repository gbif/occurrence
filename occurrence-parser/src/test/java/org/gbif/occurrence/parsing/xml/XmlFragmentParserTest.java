package org.gbif.occurrence.parsing.xml;

import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.OccurrenceKeyHelper;
import org.gbif.occurrence.common.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.model.RawOccurrenceRecord;
import org.gbif.occurrence.parsing.RawXmlOccurrence;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.io.Resources;
import org.apache.commons.io.Charsets;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class XmlFragmentParserTest {

  @Test
  public void testUtf8a() throws IOException {
    // note the collector name has an u umlaut
    String xml = Resources.toString(Resources.getResource("id_extraction/abcd1_umlaut.xml"), Charsets.UTF_8);

    RawXmlOccurrence rawRecord = createFakeOcc(xml, OccurrenceSchemaType.ABCD_1_2);
    List<RawOccurrenceRecord> results = XmlFragmentParser.parseRecord(rawRecord);
    assertEquals(1, results.size());
    // System.out.println("Looking for [Oschütz], got collector name [" + results.get(0).getCollectorName() + "]");
    assertTrue(results.get(0).getCollectorName().equals("Oschütz"));
  }

  @Test
  public void testParsingMultipleIdsUnitQualifier() throws IOException {
    // One occurrence has a unitQualifier, the other does not
    String xml = Resources.toString(Resources.getResource("id_extraction/abcd2_multi_with_null.xml"), Charsets.UTF_8);

    byte[] rawRecord = createFakeOcc(xml, OccurrenceSchemaType.ABCD_2_0_6).getXml().getBytes("UTF-8");
    String uq = "Entelophyllum ex gr. roemeri Wedekind, 1927";
    RawOccurrenceRecord resultUnit1 = XmlFragmentParser.parseRecord(rawRecord, OccurrenceSchemaType.ABCD_2_0_6, uq);
    RawOccurrenceRecord resultUnit2 = XmlFragmentParser.parseRecord(rawRecord, OccurrenceSchemaType.ABCD_2_0_6, null);
    assertEquals(uq, resultUnit1.getScientificName());
    assertNull(resultUnit2.getScientificName());
  }

  @Ignore("too expensive for constant use")
  @Test
  public void testMultiThreadLoad() throws Exception {
    String xml = Resources.toString(Resources.getResource("id_extraction/abcd1_umlaut.xml"), Charsets.UTF_8);

    // 5 parsers in 5 threads parse 1000 records each
    List<RawXmlOccurrence> raws = new ArrayList<RawXmlOccurrence>();
    for (int i = 0; i < 5000; i++) {
      raws.add(createFakeOcc(xml, OccurrenceSchemaType.ABCD_1_2));
    }

    ExecutorService tp = Executors.newFixedThreadPool(5);
    List<Future<List<RawOccurrenceRecord>>> futures = new ArrayList<Future<List<RawOccurrenceRecord>>>();
    for (int i = 0; i < 5; i++) {
      int start = i * 1000;
      int end = start + 1000;
      futures.add(tp.submit(new BatchRecordParser(raws.subList(start, end))));
    }

    List<RawOccurrenceRecord> rors = new ArrayList<RawOccurrenceRecord>();
    for (Future<List<RawOccurrenceRecord>> future : futures) {
      rors.addAll(future.get());
    }

    assertEquals(5000, rors.size());
  }

  private class BatchRecordParser implements Callable<List<RawOccurrenceRecord>> {

    private List<RawXmlOccurrence> raws;

    public BatchRecordParser(List<RawXmlOccurrence> raws) {
      this.raws = raws;
    }

    public List<RawOccurrenceRecord> call() {
      List<RawOccurrenceRecord> rors = new ArrayList<RawOccurrenceRecord>();
      for (RawXmlOccurrence raw : raws) {
        rors.addAll(XmlFragmentParser.parseRecord(raw));
      }
      return rors;
    }
  }

  @Test
  public void testIdExtractionSimple() throws IOException {
    String xml = Resources.toString(Resources.getResource("id_extraction/abcd1_simple.xml"), Charsets.UTF_8);
    UUID datasetKey = UUID.randomUUID();
    byte[] xmlBytes = xml.getBytes(Charset.forName("UTF8"));
    Set<IdentifierExtractionResult> extractionResults =
      XmlFragmentParser.extractIdentifiers(datasetKey, xmlBytes, OccurrenceSchemaType.ABCD_1_2, true,
        true);
    HolyTriplet target =
      new HolyTriplet(datasetKey, "TLMF", "Tiroler Landesmuseum Ferdinandeum", "82D45C93-B297-490E-B7B0-E0A9BEED1326",
        null);
    Set<UniqueIdentifier> ids = extractionResults.iterator().next().getUniqueIdentifiers();
    assertEquals(1, ids.size());
    UniqueIdentifier id = ids.iterator().next();
    assertEquals(datasetKey, id.getDatasetKey());
    assertEquals(OccurrenceKeyHelper.buildKey(target), id.getUniqueString());
  }

  @Test
  public void testIdExtractionMultipleIdsUnitQualifier() throws IOException {
    String xml = Resources.toString(Resources.getResource("id_extraction/abcd2_multi.xml"), Charsets.UTF_8);

    UUID datasetKey = UUID.randomUUID();
    byte[] xmlBytes = xml.getBytes(Charset.forName("UTF8"));
    Set<IdentifierExtractionResult> extractionResults =
      XmlFragmentParser.extractIdentifiers(datasetKey, xmlBytes, OccurrenceSchemaType.ABCD_2_0_6, true,
        true);
    HolyTriplet holyTriplet1 =
      new HolyTriplet(datasetKey, "BGBM", "Bridel Herbar", "Bridel-1-428", "Grimmia alpicola Sw. ex Hedw.");
    HolyTriplet holyTriplet2 = new HolyTriplet(datasetKey, "BGBM", "Bridel Herbar", "Bridel-1-428",
      "Schistidium agassizii Sull. & Lesq. in Sull.");
    assertEquals(2, extractionResults.size());
    for (IdentifierExtractionResult result : extractionResults) {
      String uniqueId = result.getUniqueIdentifiers().iterator().next().getUniqueString();
      assertTrue(uniqueId.equals(OccurrenceKeyHelper.buildKey(holyTriplet1)) || uniqueId
        .equals(OccurrenceKeyHelper.buildKey(holyTriplet2)));
    }
  }

  @Test
  public void testIdExtractionMultipleIdsUnitQualifierDuplicates() throws IOException {
    String xml = Resources.toString(Resources.getResource("id_extraction/abcd2_multi_with_null.xml"), Charsets.UTF_8);
    UUID datasetKey = UUID.randomUUID();
    byte[] xmlBytes = xml.getBytes(Charset.forName("UTF8"));
    Set<IdentifierExtractionResult> extractionResults =
      XmlFragmentParser.extractIdentifiers(datasetKey, xmlBytes, OccurrenceSchemaType.ABCD_2_0_6, true,
        true);
    HolyTriplet holyTriplet1 = new HolyTriplet(datasetKey, "Institute of Geology at TUT", "GIT", "713-13", "Entelophyllum ex gr. roemeri Wedekind, 1927");
    HolyTriplet holyTriplet2 = new HolyTriplet(datasetKey, "Institute of Geology at TUT", "GIT", "713-13", "null");
    assertEquals(2, extractionResults.size());
    for (IdentifierExtractionResult result : extractionResults) {
      String uniqueId = result.getUniqueIdentifiers().iterator().next().getUniqueString();
      System.out.println(uniqueId);
      assertTrue(uniqueId.equals(OccurrenceKeyHelper.buildKey(holyTriplet1)) || uniqueId.equals(OccurrenceKeyHelper.buildKey(holyTriplet2)));
    }
  }

  @Test
  public void testIdExtractionWithTripletAndDwcOccurrenceId() throws IOException {
    String xml = Resources.toString(Resources.getResource("id_extraction/triplet_and_dwc_id.xml"), Charsets.UTF_8);
    UUID datasetKey = UUID.randomUUID();
    byte[] xmlBytes = xml.getBytes(Charset.forName("UTF8"));
    Set<IdentifierExtractionResult> extractionResults =
      XmlFragmentParser.extractIdentifiers(datasetKey, xmlBytes, OccurrenceSchemaType.DWC_1_4, true,
        true);
    Set<UniqueIdentifier> ids = extractionResults.iterator().next().getUniqueIdentifiers();
    PublisherProvidedUniqueIdentifier pubProvided =
      new PublisherProvidedUniqueIdentifier(datasetKey, "UGENT:vertebrata:50058");
    HolyTriplet holyTriplet = new HolyTriplet(datasetKey, "UGENT", "vertebrata", "50058", null);
    assertEquals(2, ids.size());
    for (UniqueIdentifier id : ids) {
      assertTrue(id.getUniqueString().equals(OccurrenceKeyHelper.buildKey(holyTriplet)) || id.getUniqueString()
        .equals(OccurrenceKeyHelper.buildKey(pubProvided)));
    }
  }

  @Test
  public void testIdExtractTapir() throws IOException {
    String xml =
      Resources.toString(Resources.getResource("id_extraction/tapir_triplet_contains_unrecorded.xml"), Charsets.UTF_8);
    byte[] xmlBytes = xml.getBytes(Charset.forName("UTF8"));
    Set<IdentifierExtractionResult> extractionResults =
      XmlFragmentParser.extractIdentifiers(UUID.randomUUID(), xmlBytes, OccurrenceSchemaType.DWC_1_4, true,
        true);
    assertFalse(extractionResults.isEmpty());
  }

  @Test
  public void testIdExtractManisBlankCC() throws IOException {
    String xml = Resources.toString(Resources.getResource("id_extraction/manis_no_cc.xml"), Charsets.UTF_8);
    byte[] xmlBytes = xml.getBytes(Charset.forName("UTF8"));
    Set<IdentifierExtractionResult> extractionResults =
      XmlFragmentParser.extractIdentifiers(UUID.randomUUID(), xmlBytes, OccurrenceSchemaType.DWC_MANIS, true,
        true);
    assertTrue(extractionResults.isEmpty());
  }

  private RawXmlOccurrence createFakeOcc(String xml, OccurrenceSchemaType schemaType) {
    RawXmlOccurrence rawRecord = new RawXmlOccurrence();
    rawRecord.setCatalogNumber("fake catalog");
    rawRecord.setCollectionCode("fake collection code");
    rawRecord.setInstitutionCode("fake inst");
    rawRecord.setResourceName("fake resource name");
    rawRecord.setSchemaType(schemaType);
    rawRecord.setXml(xml);

    return rawRecord;
  }
}
