package org.gbif.occurrence.processor.parsing;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.parsing.xml.IdentifierExtractionResult;
import org.gbif.occurrence.persistence.api.Fragment;

import java.io.IOException;
import java.util.Date;
import java.util.Set;
import java.util.UUID;

import com.google.common.io.Resources;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.Charsets;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class JsonFragmentParserTest {

  @Test
  public void testUniqueIds() throws IOException {
    UUID datasetKey = UUID.randomUUID();
    String json = Resources.toString(Resources.getResource("uniqueids.json"), Charsets.UTF_8);
    IdentifierExtractionResult result = JsonFragmentParser.extractIdentifiers(datasetKey, json.getBytes(), true, true);
    Set<UniqueIdentifier> uniqueIds = result.getUniqueIdentifiers();
    Assert.assertEquals(2, uniqueIds.size());
    for (UniqueIdentifier uniqueId : uniqueIds) {
      if (uniqueId instanceof HolyTriplet) {
        HolyTriplet triplet = (HolyTriplet) uniqueId;
        assertEquals("ic980897", triplet.getInstitutionCode());
        assertEquals("cc1234", triplet.getCollectionCode());
        assertEquals("cn90734", triplet.getCatalogNumber());
      } else {
        PublisherProvidedUniqueIdentifier pubProvided = (PublisherProvidedUniqueIdentifier) uniqueId;
        assertEquals("occId3870", pubProvided.getPublisherProvidedIdentifier());
      }
    }
  }

  @Test
  public void testUniqueIdsNoOccId() throws IOException {
    UUID datasetKey = UUID.randomUUID();
    String json = Resources.toString(Resources.getResource("uniqueids.json"), Charsets.UTF_8);
    IdentifierExtractionResult result = JsonFragmentParser.extractIdentifiers(datasetKey, json.getBytes(), true, false);
    Set<UniqueIdentifier> uniqueIds = result.getUniqueIdentifiers();
    Assert.assertEquals(1, uniqueIds.size());
    UniqueIdentifier uniqueId = uniqueIds.iterator().next();
    HolyTriplet triplet = (HolyTriplet) uniqueId;
    assertEquals("ic980897", triplet.getInstitutionCode());
    assertEquals("cc1234", triplet.getCollectionCode());
    assertEquals("cn90734", triplet.getCatalogNumber());
  }

  @Test
  public void testUniqueIdsNoTriplet() throws IOException {
    UUID datasetKey = UUID.randomUUID();
    String json = Resources.toString(Resources.getResource("uniqueids.json"), Charsets.UTF_8);
    IdentifierExtractionResult result = JsonFragmentParser.extractIdentifiers(datasetKey, json.getBytes(), false, true);
    Set<UniqueIdentifier> uniqueIds = result.getUniqueIdentifiers();
    Assert.assertEquals(1, uniqueIds.size());
    UniqueIdentifier uniqueId = uniqueIds.iterator().next();
    PublisherProvidedUniqueIdentifier pubProvided = (PublisherProvidedUniqueIdentifier) uniqueId;
    assertEquals("occId3870", pubProvided.getPublisherProvidedIdentifier());
  }

  @Test
  public void testParseUnpreferred() throws IOException {
    UUID datasetKey = UUID.randomUUID();
    String json = Resources.toString(Resources.getResource("fragment.json"), Charsets.UTF_8);
    Fragment fragment = new Fragment(datasetKey, json.getBytes("UTF-8"), DigestUtils.md5(json.getBytes("UTF-8")),
      Fragment.FragmentType.JSON, EndpointType.DWC_ARCHIVE, new Date(), 1,
      OccurrenceSchemaType.DWCA, null, null);
    VerbatimOccurrence verb = JsonFragmentParser.parseRecord(fragment);
    assertNotNull(verb);

    /*

    assertEquals("Plantae", verb.getKingdom());
    assertNull(verb.getAuthor());
    assertEquals("2400", verb.getMinAltitude());
    assertEquals("Magnoliophyta", verb.getPhylum());
    assertEquals("BGBM", verb.getInstitutionCode());
    assertEquals("specimen", verb.getBasisOfRecord());
    assertEquals("Verbascum cheiranthifolium var. cheiranthifolium", verb.getScientificName());
    assertEquals("Pontaurus", verb.getCollectionCode());
    assertEquals("Markus Döring", verb.getCollectorName());
    assertNull(verb.getLocality());
    assertEquals("7", verb.getMonth());
    assertEquals("988", verb.getCatalogNumber());
    assertEquals("37.42", verb.getLatitude());
    assertEquals("1999", verb.getYear());
    assertEquals("Verbascum", verb.getGenus());
    assertEquals("Markus Döring", verb.getIdentifierName());
    assertEquals("Scrophulariales", verb.getOrder());
    assertEquals("30", verb.getDay());
    assertEquals("Turkey", verb.getCountry());
    assertEquals("34.568", verb.getLongitude());
    assertEquals("Magnoliopsida", verb.getKlass());
    assertEquals("Scrophulariaceae", verb.getFamily());
    assertEquals("Bosphorus", verb.getContinentOrOcean());
     */
  }

  @Test
  public void testParsePreferred() throws IOException {
    UUID datasetKey = UUID.randomUUID();
    String json = Resources.toString(Resources.getResource("fragment_preferred_fields.json"), Charsets.UTF_8);
    Fragment fragment = new Fragment(datasetKey, json.getBytes("UTF-8"), DigestUtils.md5(json.getBytes("UTF-8")),
      Fragment.FragmentType.JSON, EndpointType.DWC_ARCHIVE, new Date(), 1,
      OccurrenceSchemaType.DWCA, null, null);
    VerbatimOccurrence verb = JsonFragmentParser.parseRecord(fragment);
    assertNotNull(verb);

    /*
    assertEquals("Plantae", verb.getKingdom());
    assertNull(verb.getAuthor());
    assertEquals("2400", verb.getMinAltitude());
    assertEquals("Magnoliophyta", verb.getPhylum());
    assertEquals("BGBM", verb.getInstitutionCode());
    assertEquals("specimen", verb.getBasisOfRecord());
    assertEquals("Verbascum cheiranthifolium var. cheiranthifolium", verb.getScientificName());
    assertEquals("Pontaurus", verb.getCollectionCode());
    assertEquals("Markus Döring", verb.getCollectorName());
    assertNull(verb.getLocality());
    assertEquals("7", verb.getMonth());
    assertEquals("988", verb.getCatalogNumber());
    assertEquals("37.42123", verb.getLatitude());
    assertEquals("1999", verb.getYear());
    assertEquals("Verbascum", verb.getGenus());
    assertEquals("Markus Döring", verb.getIdentifierName());
    assertEquals("Scrophulariales", verb.getOrder());
    assertEquals("30", verb.getDay());
    assertEquals("TR", verb.getCountry());
    assertEquals("34.568123", verb.getLongitude());
    assertEquals("Magnoliopsida", verb.getKlass());
    assertEquals("Scrophulariaceae", verb.getFamily());
    assertEquals("Asia", verb.getContinentOrOcean());
     */
  }
}
