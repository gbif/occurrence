/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.file;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    //Varbatim fields not populated by Java fields must be copied into the result
    occurrence.setVerbatimField(DwcTerm.institutionCode, "INST");

    //Latitude and longitude must be superseded by the interpreted values
    occurrence.setVerbatimField(DwcTerm.decimalLatitude, "89.200001");
    occurrence.setVerbatimField(DwcTerm.decimalLongitude, "100.200001");

    MediaObject mediaObjectStillImage = new MediaObject();
    mediaObjectStillImage.setTitle("Image");
    mediaObjectStillImage.setType(MediaType.StillImage);

    MediaObject mediaObjectMovingImage = new MediaObject();
    mediaObjectMovingImage.setTitle("Video");
    mediaObjectMovingImage.setType(MediaType.MovingImage);

    List<MediaObject> mediaObjects = new ArrayList<>();
    mediaObjects.add(mediaObjectMovingImage);
    mediaObjects.add(mediaObjectStillImage);

    occurrence.setMedia(mediaObjects);
    HashSet<OccurrenceIssue> issues = new HashSet<>();
    issues.add(OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH);
    issues.add(OccurrenceIssue.TAXON_MATCH_FUZZY);
    occurrence.setIssues(issues);


    Map<String,String> occurrenceMap = OccurrenceMapReader.buildInterpretedOccurrenceMap(occurrence);

    assertEquals(Country.COSTA_RICA.getIso2LetterCode(), occurrenceMap.get(DwcTerm.countryCode.simpleName()));
    assertEquals(Country.TRINIDAD_TOBAGO.getIso2LetterCode(), occurrenceMap.get(GbifTerm.publishingCountry.simpleName()));
    assertEquals(Continent.NORTH_AMERICA.name(), occurrenceMap.get(DwcTerm.continent.simpleName()));
    assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), occurrenceMap.get(DwcTerm.basisOfRecord.simpleName()));
    assertEquals(scientificName, occurrenceMap.get(GbifTerm.acceptedScientificName.simpleName()));
    assertEquals(scientificName, occurrenceMap.get(DwcTerm.scientificName.simpleName()));
    assertEquals("1", occurrenceMap.get(GbifTerm.kingdomKey.simpleName()));
    assertEquals("2440897", occurrenceMap.get(GbifTerm.taxonKey.simpleName()));
    assertEquals(OccurrenceMapReader.toISO8601Date(today), occurrenceMap.get(GbifTerm.lastInterpreted.simpleName()));
    assertEquals("89.2", occurrenceMap.get(DwcTerm.decimalLatitude.simpleName()));
    assertEquals("100.2", occurrenceMap.get(DwcTerm.decimalLongitude.simpleName()));
    assertEquals(Boolean.TRUE.toString(), occurrenceMap.get(GbifTerm.hasCoordinate.simpleName()));
    assertEquals(Boolean.TRUE.toString(), occurrenceMap.get(GbifTerm.repatriated.simpleName()));
    assertEquals(datasetKey.toString(), occurrenceMap.get(GbifTerm.datasetKey.simpleName()));
    assertEquals(reference.toString(), occurrenceMap.get(DcTerm.references.simpleName()));
    assertEquals(License.CC_BY_4_0.name(), occurrenceMap.get(DcTerm.license.simpleName()));
    assertTrue(occurrenceMap.get(GbifTerm.mediaType.simpleName()).contains(MediaType.StillImage.name()));
    assertTrue(occurrenceMap.get(GbifTerm.mediaType.simpleName()).contains(MediaType.MovingImage.name()));
    assertTrue(occurrenceMap.get(GbifTerm.issue.simpleName()).contains(OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH.name()));
    assertTrue(occurrenceMap.get(GbifTerm.issue.simpleName()).contains(OccurrenceIssue.TAXON_MATCH_FUZZY.name()));
    assertEquals(Boolean.TRUE.toString(), occurrenceMap.get(GbifTerm.hasGeospatialIssues.simpleName()));
    assertEquals(occurrenceMap.get(DwcTerm.institutionCode.simpleName()), "INST");
    assertEquals(occurrenceMap.get(DwcTerm.decimalLatitude.simpleName()), "89.2");
    assertEquals(occurrenceMap.get(DwcTerm.decimalLongitude.simpleName()), "100.2");
  }
}
