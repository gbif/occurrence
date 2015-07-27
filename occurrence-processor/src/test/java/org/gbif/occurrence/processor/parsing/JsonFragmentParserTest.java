package org.gbif.occurrence.processor.parsing;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.parsing.xml.IdentifierExtractionResult;
import org.gbif.occurrence.persistence.api.Fragment;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.io.Resources;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.Charsets;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
  public void testParsePreferred() throws IOException {
    UUID datasetKey = UUID.randomUUID();
    String json = Resources.toString(Resources.getResource("fragment.json"), Charsets.UTF_8);
    Fragment fragment = new Fragment(datasetKey, json.getBytes("UTF-8"), DigestUtils.md5(json.getBytes("UTF-8")),
      Fragment.FragmentType.JSON, EndpointType.DWC_ARCHIVE, new Date(), 1,
      OccurrenceSchemaType.DWCA, null, null);
    VerbatimOccurrence verb = JsonFragmentParser.parseRecord(fragment);
    assertNotNull(verb);

    assertEquals("Plantae", verb.getVerbatimField(DwcTerm.kingdom));
    assertNull(verb.getVerbatimField(DwcTerm.scientificNameAuthorship));
    assertEquals("2400", verb.getVerbatimField(DwcTerm.minimumElevationInMeters));
    assertEquals("Magnoliophyta", verb.getVerbatimField(DwcTerm.phylum));
    assertEquals("BGBM", verb.getVerbatimField(DwcTerm.institutionCode));
    assertEquals("specimen", verb.getVerbatimField(DwcTerm.basisOfRecord));
    assertEquals("Verbascum cheiranthifolium var. cheiranthifolium", verb.getVerbatimField(DwcTerm.scientificName));
    assertEquals("Pontaurus", verb.getVerbatimField(DwcTerm.collectionCode));
    assertEquals("Markus Döring", verb.getVerbatimField(DwcTerm.recordedBy));
    assertNull(verb.getVerbatimField(DwcTerm.locality));
    assertEquals("7", verb.getVerbatimField(DwcTerm.month));
    assertEquals("988", verb.getVerbatimField(DwcTerm.catalogNumber));
    assertEquals("37.42123", verb.getVerbatimField(DwcTerm.decimalLatitude));
    assertEquals("1999", verb.getVerbatimField(DwcTerm.year));
    assertEquals("Verbascum", verb.getVerbatimField(DwcTerm.genus));
    assertEquals("Markus Döring", verb.getVerbatimField(DwcTerm.identifiedBy));
    assertEquals("Scrophulariales", verb.getVerbatimField(DwcTerm.order));
    assertEquals("30", verb.getVerbatimField(DwcTerm.day));
    assertEquals("Fake", verb.getVerbatimField(DwcTerm.country));
    assertEquals("TR", verb.getVerbatimField(DwcTerm.countryCode));
    assertEquals("34.568123", verb.getVerbatimField(DwcTerm.decimalLongitude));
    assertEquals("Magnoliopsida", verb.getVerbatimField(DwcTerm.class_));
    assertEquals("Scrophulariaceae", verb.getVerbatimField(DwcTerm.family));
    assertEquals("Asia", verb.getVerbatimField(DwcTerm.continent));
    assertNull(verb.getVerbatimField(TermFactory.instance().findTerm("extensions")));

    // test image extension
    for (Extension ext : Extension.values()) {
      if (ext == Extension.IMAGE) continue;
      assertFalse(verb.getExtensions().containsKey(ext));
    }
    assertTrue(verb.getExtensions().containsKey(Extension.IMAGE));
    assertEquals(1, verb.getExtensions().get(Extension.IMAGE).size());
    assertEquals("http://digit.snm.ku.dk/www/Aves/full/AVES-100348_Caprimulgus_pectoralis_fervidus_ad____f.jpg", verb.getExtensions().get(Extension.IMAGE).get(0).get(DcTerm.identifier));
  }

    @Test
    public void testExtensions() throws IOException {
        UUID datasetKey = UUID.randomUUID();
        String json = Resources.toString(Resources.getResource("fragment-extensions.json"), Charsets.UTF_8);
        Fragment fragment = new Fragment(datasetKey, json.getBytes("UTF-8"), DigestUtils.md5(json.getBytes("UTF-8")),
                Fragment.FragmentType.JSON, EndpointType.DWC_ARCHIVE, new Date(), 1,
                OccurrenceSchemaType.DWCA, null, null);
        VerbatimOccurrence verb = JsonFragmentParser.parseRecord(fragment);
        assertNotNull(verb);

        assertEquals("http://collections.mnh.si.edu/media/index.php?irn=10842031", verb.getVerbatimField(DwcTerm.associatedMedia));
        // test extensions
        //TODO: The EOL extension is not recognized as the dwc-api dependency is outdated and does not contain EOL terms...
        // once updated please change the size below to 3 and outcomment the eol assertions
        assertEquals(2, verb.getExtensions().size());
        // test media extension
        assertEquals(2, verb.getExtensions().get(Extension.MULTIMEDIA).size());
        Map<Term, String> m = verb.getExtensions().get(Extension.MULTIMEDIA).get(0);
        assertEquals("http://www.mnh.si.edu/rc/db/2data_access_policy.html", m.get(DcTerm.license));
        assertEquals("USNMENT832289", m.get(DcTerm.description));
        assertEquals("National Museum of Natural History, Smithsonian Institution", m.get(DcTerm.publisher));
        assertEquals("http://collections.mnh.si.edu/media/index.php?irn=10842031", m.get(DcTerm.identifier));
        assertEquals("Trichardis picta", m.get(DcTerm.title));
        assertEquals("Image", m.get(DcTerm.type));
        assertEquals("National Museum of Natural History, Smithsonian Institution", m.get(DcTerm.rightsHolder));
        assertEquals("general public", m.get(DcTerm.audience));
        m = verb.getExtensions().get(Extension.MULTIMEDIA).get(1);
        assertEquals("http://collections.mnh.si.edu/media/index.php?irn=10842031b", m.get(DcTerm.identifier));
        // test EOL extension
        //TODO: outcomment once dwc-api is updated
        //assertEquals(1, verb.getExtensions().get(Extension.EOL_MEDIA).size());
        //m = verb.getExtensions().get(Extension.EOL_MEDIA).get(0);
        //assertEquals("http://www.mnh.si.edu/rc/db/2data_access_policy.html", m.get(DcTerm.license));
        //assertEquals("USNMENT832289", m.get(DcTerm.description));
        //assertEquals("National Museum of Natural History, Smithsonian Institution", m.get(DcTerm.publisher));
        //assertEquals("http://collections.mnh.si.edu/media/index.php?irn=10842031c", m.get(DcTerm.identifier));
        //assertEquals("Trichardis picta", m.get(DcTerm.title));
        //assertEquals("Image", m.get(DcTerm.type));
        //assertEquals("National Museum of Natural History, Smithsonian Institution", m.get(DcTerm.rightsHolder));
        //assertEquals("general public", m.get(DcTerm.audience));
        // test identified extension
        assertEquals(1, verb.getExtensions().get(Extension.IDENTIFICATION).size());
        m = verb.getExtensions().get(Extension.IDENTIFICATION).get(0);
        assertEquals("Marta marta", m.get(DwcTerm.scientificName));
        assertEquals("T Bone", m.get(DwcTerm.identifiedBy));
    }
}
