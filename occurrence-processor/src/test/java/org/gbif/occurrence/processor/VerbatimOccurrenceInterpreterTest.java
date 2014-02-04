package org.gbif.occurrence.processor;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrencePersistenceStatus;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpretationResult;
import org.gbif.occurrence.processor.interpreting.VerbatimOccurrenceInterpreter;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.UUID;

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Charsets;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.test.TestingServer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Ignore("requires real webservices and messaging")
public class VerbatimOccurrenceInterpreterTest {

  // BoGART from BGBM
  private final UUID DATASET_KEY = UUID.fromString("85697f04-f762-11e1-a439-00145eb45e9a");
  // BGBM
  private final UUID OWNING_ORG_KEY = UUID.fromString("57254bd0-8256-11d8-b7ed-b8a03c50a862");
  private final long MODIFIED = System.currentTimeMillis();

  private VerbatimOccurrence verb;
  private VerbatimOccurrence verbMod;
  private VerbatimOccurrenceInterpreter interpreter;
  private static MessagePublisher messagePublisher;
  private TestingServer zkServer;
  private CuratorFramework curator;
  private ZookeeperConnector zookeeperConnector;

  @BeforeClass
  public static void preClass() throws IOException {
    ConnectionParameters connectionParameters = new ConnectionParameters("localhost", 5672, "guest", "guest", "/");
    messagePublisher = new DefaultMessagePublisher(connectionParameters);
  }

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

    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setKey(fragment.getKey());
    v.setDatasetKey(DATASET_KEY);
    v.setLastCrawled(new Date(MODIFIED));
    v.setProtocol(EndpointType.DWC_ARCHIVE);
    v.setPublishingOrgKey(OWNING_ORG_KEY);
    v.setPublishingCountry(Country.GERMANY);
    v.setField(DwcTerm.scientificNameAuthorship, "Linneaus");
    v.setField(DwcTerm.basisOfRecord, "specimen");
    v.setField(DwcTerm.recordedBy, "Hobern");
    v.setField(DwcTerm.continent, "Europe");
    v.setField(DwcTerm.country, "Danmark");
    v.setField(DwcTerm.county, "Copenhagen");
    v.setField(DwcTerm.catalogNumber, "cn");
    v.setField(DwcTerm.collectionCode, "cc");
    v.setField(DwcTerm.dateIdentified, "10-11-12");
    v.setField(DwcTerm.day, "22");
    v.setField(DwcTerm.family, "Felidae");
    v.setField(DwcTerm.genus, "Panthera");
    v.setField(DwcTerm.identifiedBy, "Hobern");
    v.setField(DwcTerm.institutionCode, "ic");
    v.setField(DwcTerm.kingdom, "Animalia");
    v.setField(DwcTerm.class_, "Mammalia");
    v.setField(DwcTerm.decimalLatitude, "55.6750");
    v.setField(DwcTerm.decimalLongitude, "12.5687");
    v.setField(DwcTerm.coordinatePrecision, "20.123");
    v.setField(DwcTerm.locality, "copenhagen");
    v.setField(DwcTerm.maximumElevationInMeters, "1200");
    v.setField(DwcTerm.maximumDepthInMeters, "500");
    v.setField(DwcTerm.minimumElevationInMeters, "100");
    v.setField(DwcTerm.minimumDepthInMeters, "20");
    v.setField(DwcTerm.month, "4");
    v.setField(DwcTerm.eventDate, "1990-04-22");
    v.setField(DwcTerm.order, "Carnivora");
    v.setField(DwcTerm.phylum, "Chordata");
    v.setField(DwcTerm.taxonRank, "Species");
    v.setField(DwcTerm.scientificName, "Panthera onca onca");
    v.setField(DwcTerm.specificEpithet, "onca");
    v.setField(DwcTerm.infraspecificEpithet, "onca");
    v.setField(DwcTerm.stateProvince, "Copenhagen");
    v.setField(DwcTerm.year, "1990");
    v.setField(DwcTerm.collectionCode, "cc");

    verb = v;

    //TODO: make copy of v and replace scientificName!!!
    verbMod = v; //builder.scientificName("Panthera onca goldmani").build();
  }

  @Test
  public void testFullNew() {
    // TODO: continent, geospatial issue, other issue
    OccurrenceInterpretationResult interpResult = interpreter.interpret(verb, OccurrencePersistenceStatus.NEW, true);
    assertNotNull(interpResult);
    Occurrence result = interpResult.getUpdated();
    assertEquals(verb.getKey(), result.getKey());
    assertEquals(650, result.getAltitude().intValue());
    assertEquals(BasisOfRecord.PRESERVED_SPECIMEN, result.getBasisOfRecord());
    assertEquals("cn", result.getField(DwcTerm.catalogNumber));
    assertEquals("cc", result.getField(DwcTerm.collectionCode));
    assertEquals("Hobern", result.getField(DwcTerm.recordedBy));
    assertEquals(Country.fromIsoCode("DK"), result.getCountry());
    assertEquals("Copenhagen", result.getField(DwcTerm.county));
    assertEquals(DATASET_KEY, result.getDatasetKey());
    assertEquals(260, result.getDepth().intValue());
    assertEquals("Felidae", result.getFamily());
    assertEquals(9703, result.getFamilyKey().intValue());
    assertEquals("Panthera", result.getGenus());
    assertEquals(2435194, result.getGenusKey().intValue());
    assertEquals("Hobern", result.getField(DwcTerm.identifiedBy));
    assertEquals("ic", result.getField(DwcTerm.institutionCode));
    assertEquals(1, result.getKey().intValue());
    assertEquals("Animalia", result.getKingdom());
    assertEquals(1, result.getKingdomKey().intValue());
    assertEquals(Double.valueOf(55.6750), result.getLatitude());
    assertEquals("copenhagen", result.getField(DwcTerm.locality));
    assertEquals(Double.valueOf(12.5687), result.getLongitude());
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
    assertEquals("Copenhagen", result.getField(DwcTerm.county));
    assertEquals("Panthera onca", result.getSpecies());
    assertEquals(5219426, result.getSpeciesKey().intValue());
    assertNull(result.getSubgenus());
    assertNull(result.getSubgenusKey());
    assertNull(result.getField(GbifTerm.unitQualifier));
    assertEquals(Country.GERMANY, result.getPublishingCountry());
    assertEquals(EndpointType.DWC_ARCHIVE, result.getProtocol());
  }

  @Test
  public void testUpdate() {
    interpreter.interpret(verb, OccurrencePersistenceStatus.NEW, true);
    OccurrenceInterpretationResult interpResultMod = interpreter.interpret(verbMod, OccurrencePersistenceStatus.UPDATED, true);
    System.out.println("original\n" + interpResultMod.getOriginal().toString());
    System.out.println("updated\n" + interpResultMod.getUpdated().toString());
    assertNotNull(interpResultMod.getUpdated());
    assertNotNull(interpResultMod.getOriginal());
  }
}
