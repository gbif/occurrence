package org.gbif.occurrence.download.file;

import com.google.common.collect.Lists;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.License;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.common.TermUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.UUID;


/**
 * Test cases for the class {@link OccurrenceMapReader}.
 */
public class OccurrenceMapReaderTest {

  /**
   * Occurrence map test, uses a combination of different set of data types a and terms to check most of the cases.
   */
  @Test
  public void buildOccurrenceMapTest() {
    String scientificName = "Tapirus bairdii (Gill, 1865)";
    UUID datasetKey = UUID.randomUUID();
    Date today = new Date();

    URI reference = URI.create("http://www.gbif.org");
    Occurrence occurrence = new Occurrence();
    occurrence.setBasisOfRecord(BasisOfRecord.HUMAN_OBSERVATION);
    occurrence.setAcceptedScientificName(scientificName);
    occurrence.setScientificName(scientificName);
    occurrence.setContinent(Continent.NORTH_AMERICA);
    occurrence.setCountry(Country.COSTA_RICA);
    occurrence.setPublishingCountry(Country.TRINIDAD_TOBAGO);
    occurrence.setKingdomKey(1);
    occurrence.setTaxonKey(2440897);
    occurrence.setLastInterpreted(today);
    occurrence.setDecimalLatitude(89.2);
    occurrence.setDecimalLongitude(100.2);
    occurrence.setDatasetKey(datasetKey);
    occurrence.setReferences(reference);
    occurrence.setLicense(License.CC_BY_4_0);

    Map<String,String> occurrenceMap = OccurrenceMapReader.buildOccurrenceMap(occurrence, Lists.newArrayList(TermUtils.interpretedTerms()));

    Assert.assertEquals(Country.COSTA_RICA.getIso2LetterCode(), occurrenceMap.get(DwcTerm.countryCode.simpleName()));
    Assert.assertEquals(Country.TRINIDAD_TOBAGO.getIso2LetterCode(), occurrenceMap.get(GbifTerm.publishingCountry.simpleName()));
    Assert.assertEquals(Continent.NORTH_AMERICA.name(), occurrenceMap.get(DwcTerm.continent.simpleName()));
    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), occurrenceMap.get(DwcTerm.basisOfRecord.simpleName()));
    Assert.assertEquals(scientificName, occurrenceMap.get(GbifTerm.acceptedScientificName.simpleName()));
    Assert.assertEquals(scientificName, occurrenceMap.get(DwcTerm.scientificName.simpleName()));
    Assert.assertEquals("1", occurrenceMap.get(GbifTerm.kingdomKey.simpleName()));
    Assert.assertEquals("2440897", occurrenceMap.get(GbifTerm.taxonKey.simpleName()));
    Assert.assertEquals(OccurrenceMapReader.toISO8601Date(today), occurrenceMap.get(GbifTerm.lastInterpreted.simpleName()));
    Assert.assertEquals("89.2", occurrenceMap.get(DwcTerm.decimalLatitude.simpleName()));
    Assert.assertEquals("100.2", occurrenceMap.get(DwcTerm.decimalLongitude.simpleName()));
    Assert.assertEquals(Boolean.TRUE.toString(), occurrenceMap.get(GbifTerm.hasCoordinate.simpleName()));
    Assert.assertEquals(Boolean.TRUE.toString(), occurrenceMap.get(GbifTerm.repatriated.simpleName()));
    Assert.assertEquals(datasetKey.toString(), occurrenceMap.get(GbifTerm.datasetKey.simpleName()));
    Assert.assertEquals(reference.toString(), occurrenceMap.get(DcTerm.references.simpleName()));
    Assert.assertEquals(License.CC_BY_4_0.name(), occurrenceMap.get(DcTerm.license.simpleName()));
  }
}
