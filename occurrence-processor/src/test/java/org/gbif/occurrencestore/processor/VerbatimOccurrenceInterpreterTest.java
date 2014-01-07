package org.gbif.occurrencestore.processor;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.OccurrencePersistenceStatus;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrencestore.common.model.HolyTriplet;
import org.gbif.occurrencestore.common.model.UniqueIdentifier;
import org.gbif.occurrencestore.persistence.api.Fragment;
import org.gbif.occurrencestore.persistence.api.FragmentPersistenceService;
import org.gbif.occurrencestore.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrence;
import org.gbif.occurrencestore.processor.interpreting.InterpretationResult;
import org.gbif.occurrencestore.processor.interpreting.VerbatimOccurrenceInterpreter;
import org.gbif.occurrencestore.processor.zookeeper.ZookeeperConnector;

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
    VerbatimOccurrence.Builder builder =
      VerbatimOccurrence.builder().altitudePrecision("10").author("Linneaus").basisOfRecord("specimen")
        .collectorName("Hobern").continentOrOcean("Europe").country("Danmark").county("Copenhagen").catalogNumber("cn")
        .collectionCode("cc").dataProviderId(123).dataResourceId(456).dateIdentified("10-11-12").day("22")
        .dayIdentified("10").depthPrecision("10").datasetKey(DATASET_KEY).family("Felidae").genus("Panthera")
        .key(fragment.getKey()).identifierName("Hobern").institutionCode("ic").kingdom("Animalia").klass("Mammalia")
        .latitude("55.6750").latLongPrecision("20.123").locality("copenhagen").longitude("12.5687").maxAltitude("1200")
        .maxDepth("500").minAltitude("100").minDepth("20").modified(MODIFIED).month("4").monthIdentified("11")
        .occurrenceDate("1990-04-22").order("Carnivora").phylum("Chordata")
        .protocol(EndpointType.DWC_ARCHIVE).rank("Species").resourceAccessPointId(890)
        .scientificName("Panthera onca onca").species("onca").stateOrProvince("Copenhagen").subspecies("onca")
        .year("1990").yearIdentified("2012");

    verb = builder.scientificName("Panthera onca onca").build();
    verbMod = builder.scientificName("Panthera onca goldmani").build();
  }

  @Test
  public void testFullNew() {
    InterpretationResult interpResult = interpreter.interpret(verb, OccurrencePersistenceStatus.NEW, true);
    assertNotNull(interpResult);
    Occurrence result = interpResult.getUpdated();
    assertEquals(650, result.getAltitude().intValue());
    assertEquals(BasisOfRecord.PRESERVED_SPECIMEN, result.getBasisOfRecord());
//    assertEquals("cn", result.getCatalogNumber());
//    assertEquals("cc", result.getCollectionCode());
//    assertEquals("Hobern", result.getCollectorName());
//    assertEquals("Europe", result.getContinent());
    assertEquals(Country.fromIsoCode("DK"), result.getCountry());
//    assertEquals("Copenhagen", result.getCounty());
//    assertEquals(123, result.getDataProviderId().intValue());
//    assertEquals(456, result.getDataResourceId().intValue());
    assertEquals(DATASET_KEY, result.getDatasetKey());
    assertEquals(260, result.getDepth().intValue());
    assertEquals("Felidae", result.getFamily());
    assertEquals(9703, result.getFamilyKey().intValue());
//    assertEquals(0, result.getGeospatialIssue().intValue());
    assertEquals("Panthera", result.getGenus());
    assertEquals(2435194, result.getGenusKey().intValue());
//    assertEquals("Hobern", result.getIdentifierName());
    // TODO: identifiers
    //    assertEquals(, result.getIdentifiers());
//    assertEquals("ic", result.getInstitutionCode());
    assertEquals(1, result.getKey().intValue());
    assertEquals("Animalia", result.getKingdom());
    assertEquals(1, result.getKingdomKey().intValue());
    assertEquals(Double.valueOf(55.6750), result.getLatitude());
//    assertEquals("copenhagen", result.getLocality());
    assertEquals(Double.valueOf(12.5687), result.getLongitude());
    assertTrue(MODIFIED <= result.getModified().getTime());
//    assertEquals(7193916, result.getNubKey().intValue());
    Calendar cal = Calendar.getInstance();
    cal.set(1990, 3, 22);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
//    assertEquals(cal.getTime(), result.getOccurrenceDate());
//    assertEquals(4, result.getOccurrenceMonth().intValue());
//    assertEquals(1990, result.getOccurrenceYear().intValue());
//    assertEquals(0, result.getOtherIssue().intValue());
//    assertEquals(OWNING_ORG_KEY, result.getOwningOrgKey());
    assertEquals("Carnivora", result.getOrder());
    assertEquals(732, result.getOrderKey().intValue());
    assertEquals("Chordata", result.getPhylum());
    assertEquals(44, result.getPhylumKey().intValue());
//    assertEquals(890, result.getResourceAccessPointId().intValue());
    assertEquals("Panthera onca subsp. onca", result.getScientificName());
    assertEquals("Copenhagen", result.getStateProvince());
    assertEquals("Panthera onca", result.getSpecies());
    assertEquals(5219426, result.getSpeciesKey().intValue());
    assertNull(result.getSubgenus());
    assertNull(result.getSubgenusKey());
//    assertEquals(0, result.getTaxonomicIssue().intValue());
//    assertNull(result.getUnitQualifier());
//    assertEquals(Country.GERMANY, result.getHostCountry());
    assertEquals(EndpointType.DWC_ARCHIVE, result.getProtocol());
  }

  @Test
  public void testUpdate() {
    interpreter.interpret(verb, OccurrencePersistenceStatus.NEW, true);
    InterpretationResult interpResultMod = interpreter.interpret(verbMod, OccurrencePersistenceStatus.UPDATED, true);
    System.out.println("original\n" + interpResultMod.getOriginal().toString());
    System.out.println("updated\n" + interpResultMod.getUpdated().toString());
    assertNotNull(interpResultMod.getUpdated());
    assertNotNull(interpResultMod.getOriginal());
  }
}
