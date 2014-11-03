package org.gbif.occurrence.processor;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrencePersistenceStatus;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpretationResult;
import org.gbif.occurrence.processor.interpreting.VerbatimOccurrenceInterpreter;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.UUID;

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

//@Ignore("requires real webservices")
public class VerbatimOccurrenceInterpreterTest {

  // BoGART from BGBM
  private final UUID DATASET_KEY = UUID.fromString("85697f04-f762-11e1-a439-00145eb45e9a");
  // BGBM
  private final UUID OWNING_ORG_KEY = UUID.fromString("57254bd0-8256-11d8-b7ed-b8a03c50a862");
  private final long MODIFIED = System.currentTimeMillis();

  private VerbatimOccurrence verb;
  private VerbatimOccurrence verbMod;
  private VerbatimOccurrenceInterpreter interpreter;
  private TestingServer zkServer;
  private CuratorFramework curator;
  private ZookeeperConnector zookeeperConnector;

  @Before
  public void setUp() throws Exception {
    zkServer = new TestingServer();
    curator = CuratorFrameworkFactory.builder().connectString(zkServer.getConnectString()).namespace("crawler")
      .retryPolicy(new RetryNTimes(1, 1000)).build();
    curator.start();
    zookeeperConnector = new ZookeeperConnector(curator);

    FragmentPersistenceService fragmentPersister =
      new FragmentPersistenceServiceMock(new OccurrenceKeyPersistenceServiceMock());
    Fragment fragment = new Fragment(DATASET_KEY, "fake".getBytes(Charsets.UTF_8), "fake".getBytes(Charsets.UTF_8),
      Fragment.FragmentType.JSON, EndpointType.DWC_ARCHIVE, new Date(), 1, null, null, new Date().getTime());
    Set<UniqueIdentifier> uniqueIds = Sets.newHashSet();
    uniqueIds.add(new HolyTriplet(DATASET_KEY, "ic", "cc", "cn", null));
    fragmentPersister.insert(fragment, uniqueIds);
    OccurrencePersistenceService occurrenceService = new OccurrencePersistenceServiceMock(fragmentPersister);
    interpreter = new VerbatimOccurrenceInterpreter(occurrenceService, zookeeperConnector);

    verb = buildVerbatim(fragment.getKey());

    verbMod = buildVerbatim(fragment.getKey());
    verbMod.setVerbatimField(DwcTerm.scientificName, "Panthera onca goldmani");
  }

  private VerbatimOccurrence buildVerbatim(int key) {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setKey(key);
    v.setDatasetKey(DATASET_KEY);
    v.setLastCrawled(new Date(MODIFIED));
    v.setProtocol(EndpointType.DWC_ARCHIVE);
    v.setPublishingOrgKey(OWNING_ORG_KEY);
    v.setPublishingCountry(Country.GERMANY);
    v.setVerbatimField(GbifTerm.gbifID, String.valueOf(key));
    v.setVerbatimField(DwcTerm.scientificNameAuthorship, "Linneaus");
    v.setVerbatimField(DwcTerm.basisOfRecord, "specimen");
    v.setVerbatimField(DwcTerm.recordedBy, "Hobern");
    v.setVerbatimField(DwcTerm.continent, "Europe");
    v.setVerbatimField(DwcTerm.country, "Danmark");
    v.setVerbatimField(DwcTerm.county, "Copenhagen");
    v.setVerbatimField(DwcTerm.catalogNumber, "cn");
    v.setVerbatimField(DwcTerm.collectionCode, "cc");
    v.setVerbatimField(DwcTerm.dateIdentified, "10-11-12");
    v.setVerbatimField(DwcTerm.day, "22");
    v.setVerbatimField(DwcTerm.family, "Felidae");
    v.setVerbatimField(DwcTerm.genus, "Panthera");
    v.setVerbatimField(DwcTerm.identifiedBy, "Hobern");
    v.setVerbatimField(DwcTerm.institutionCode, "ic");
    v.setVerbatimField(DwcTerm.kingdom, "Animalia");
    v.setVerbatimField(DwcTerm.class_, "Mammalia");
    v.setVerbatimField(DwcTerm.decimalLatitude, "55.6750");
    v.setVerbatimField(DwcTerm.decimalLongitude, "12.5687");
    v.setVerbatimField(DwcTerm.coordinatePrecision, "20.123");
    v.setVerbatimField(DwcTerm.locality, "copenhagen");
    v.setVerbatimField(DwcTerm.maximumElevationInMeters, "1200");
    v.setVerbatimField(DwcTerm.maximumDepthInMeters, "500");
    v.setVerbatimField(DwcTerm.minimumElevationInMeters, "100");
    v.setVerbatimField(DwcTerm.minimumDepthInMeters, "20");
    v.setVerbatimField(DwcTerm.month, "4");
    v.setVerbatimField(DwcTerm.eventDate, "1990-04-22");
    v.setVerbatimField(DwcTerm.order, "Carnivora");
    v.setVerbatimField(DwcTerm.phylum, "Chordata");
    v.setVerbatimField(DwcTerm.taxonRank, "Species");
    v.setVerbatimField(DwcTerm.scientificName, "Panthera onca onca");
    v.setVerbatimField(DwcTerm.specificEpithet, "onca");
    v.setVerbatimField(DwcTerm.infraspecificEpithet, "onca");
    v.setVerbatimField(DwcTerm.stateProvince, "Copenhagen");
    v.setVerbatimField(DwcTerm.year, "1990");
    v.setVerbatimField(DwcTerm.collectionCode, "cc");

    return v;
  }

  @Test
  public void testFullNew() {
    OccurrenceInterpretationResult interpResult = interpreter.interpret(verb, OccurrencePersistenceStatus.NEW, true);
    assertNotNull(interpResult);
    Occurrence result = interpResult.getUpdated();
    assertEquals(verb.getKey(), result.getKey());
    assertEquals(verb.getKey().toString(), result.getVerbatimField(GbifTerm.gbifID));
    assertEquals(650, result.getElevation().intValue());
    assertEquals(BasisOfRecord.PRESERVED_SPECIMEN, result.getBasisOfRecord());
    assertEquals("cn", result.getVerbatimField(DwcTerm.catalogNumber));
    assertEquals("cc", result.getVerbatimField(DwcTerm.collectionCode));
    assertEquals("Hobern", result.getVerbatimField(DwcTerm.recordedBy));
    assertEquals(Country.fromIsoCode("DK"), result.getCountry());
    assertEquals("Copenhagen", result.getVerbatimField(DwcTerm.county));
    assertEquals(DATASET_KEY, result.getDatasetKey());
    assertEquals(260, result.getDepth().intValue());
    assertEquals("Felidae", result.getFamily());
    assertEquals(9703, result.getFamilyKey().intValue());
    assertEquals("Panthera", result.getGenus());
    assertEquals(2435194, result.getGenusKey().intValue());
    assertEquals("Hobern", result.getVerbatimField(DwcTerm.identifiedBy));
    assertEquals("ic", result.getVerbatimField(DwcTerm.institutionCode));
    assertEquals(1, result.getKey().intValue());
    assertEquals("Animalia", result.getKingdom());
    assertEquals(1, result.getKingdomKey().intValue());
    assertEquals(Double.valueOf(55.6750), result.getDecimalLatitude());
    assertEquals("copenhagen", result.getVerbatimField(DwcTerm.locality));
    assertEquals(Double.valueOf(12.5687), result.getDecimalLongitude());
    assertTrue(MODIFIED <= result.getLastInterpreted().getTime());
    assertEquals(7193916, result.getTaxonKey().intValue());
    Calendar cal = Calendar.getInstance();
    cal.set(1990, 3, 22);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    assertEquals(cal.getTime(), result.getEventDate());
    assertEquals(4, result.getMonth().intValue());
    assertEquals(1990, result.getYear().intValue());
    assertEquals(OWNING_ORG_KEY, result.getPublishingOrgKey());
    assertEquals("Carnivora", result.getOrder());
    assertEquals(732, result.getOrderKey().intValue());
    assertEquals("Chordata", result.getPhylum());
    assertEquals(44, result.getPhylumKey().intValue());
    assertEquals("Panthera onca subsp. onca", result.getScientificName());
    assertEquals("Copenhagen", result.getVerbatimField(DwcTerm.county));
    assertEquals("Panthera onca", result.getSpecies());
    assertEquals(5219426, result.getSpeciesKey().intValue());
    assertNull(result.getSubgenus());
    assertNull(result.getSubgenusKey());
    assertNull(result.getVerbatimField(GbifInternalTerm.unitQualifier));
    assertEquals(Country.GERMANY, result.getPublishingCountry());
    assertEquals(EndpointType.DWC_ARCHIVE, result.getProtocol());
    assertEquals(Continent.EUROPE, result.getContinent());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84));
  }

  @Test
  public void testUpdate() {
    interpreter.interpret(verb, OccurrencePersistenceStatus.NEW, true);
    OccurrenceInterpretationResult interpResultMod = interpreter.interpret(verbMod, OccurrencePersistenceStatus.UPDATED, true);
    assertNotNull(interpResultMod.getUpdated());
    assertNotNull(interpResultMod.getOriginal());
  }
}
