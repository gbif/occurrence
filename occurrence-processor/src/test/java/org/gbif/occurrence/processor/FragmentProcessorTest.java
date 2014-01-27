package org.gbif.occurrence.processor;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.test.TestingServer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.Charsets;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

//@Ignore("requires real messaging")
public class FragmentProcessorTest {

  private FragmentProcessor fragmentProcessor;
  private FragmentPersistenceService fragmentPersistenceService;
  private OccurrenceKeyPersistenceService occurrenceKeyService;
  private MessagePublisher messagePublisher;
  private TestingServer zkServer;
  private CuratorFramework curator;
  private ZookeeperConnector zookeeperConnector;

  private static String abcd206Single;
  private static String abcd206Multi;
  private static String abcd206MultiModified;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void preClass() throws IOException {
    abcd206Single = Resources.toString(Resources.getResource("abcd206_single.xml"), Charsets.UTF_8);
    abcd206Multi = Resources.toString(Resources.getResource("abcd206_multi.xml"), Charsets.UTF_8);
    abcd206MultiModified = Resources.toString(Resources.getResource("abcd206_multi_modified.xml"), Charsets.UTF_8);
  }

  @Before
  public void setUp() throws Exception {
    ConnectionParameters connectionParameters = new ConnectionParameters("localhost", 5672, "guest", "guest", "/");
    messagePublisher = new DefaultMessagePublisher(connectionParameters);

    zkServer = new TestingServer();
    curator = CuratorFrameworkFactory.builder().connectString(zkServer.getConnectString()).namespace("crawler")
      .retryPolicy(new RetryNTimes(1, 1000)).build();
    curator.start();
    zookeeperConnector = new ZookeeperConnector(curator);

    occurrenceKeyService = new OccurrenceKeyPersistenceServiceMock();
    fragmentPersistenceService = new FragmentPersistenceServiceMock(occurrenceKeyService);
    fragmentProcessor =
      new FragmentProcessor(fragmentPersistenceService, occurrenceKeyService, messagePublisher, zookeeperConnector);
  }

  @After
  public void tearDown() throws IOException {
    zkServer.stop();
    curator.close();
  }

  // TODO test lookup points to nothing

  @Test
  public void testNewAbcd206Single() {
    UUID datasetKey = UUID.randomUUID();
    OccurrenceSchemaType schemaType = OccurrenceSchemaType.ABCD_2_0_6;
    EndpointType protocol = EndpointType.BIOCASE;
    Integer crawlId = 1;
    fragmentProcessor.buildFragments(datasetKey, abcd206Single.getBytes(), schemaType, protocol, crawlId, null);

    Fragment got = fragmentPersistenceService.get(1);
    assertTrue(got.getKey() > 0);
    Calendar resultCal = Calendar.getInstance();
    resultCal.setTime(got.getHarvestedDate());
    Calendar cal = Calendar.getInstance();
    assertEquals(cal.get(Calendar.DAY_OF_YEAR), resultCal.get(Calendar.DAY_OF_YEAR));
    assertEquals(Fragment.FragmentType.XML, got.getFragmentType());
    assertEquals(schemaType, got.getXmlSchema());
    assertEquals(datasetKey, got.getDatasetKey());
    assertEquals(protocol, got.getProtocol());
    assertEquals(crawlId, got.getCrawlId());
    assertTrue(Arrays.equals(abcd206Single.getBytes(), got.getData()));
    assertTrue(Arrays.equals(DigestUtils.md5(abcd206Single), got.getDataHash()));
    assertNull(got.getUnitQualifier());
  }

  @Test
  public void testNewAbcd206Multiple() {
    UUID datasetKey = UUID.randomUUID();
    OccurrenceSchemaType schemaType = OccurrenceSchemaType.ABCD_2_0_6;
    Integer crawlId = 1;
    fragmentProcessor
      .buildFragments(datasetKey, abcd206Multi.getBytes(), schemaType, EndpointType.BIOCASE, crawlId, null);
    Set<Fragment> fragments = Sets.newHashSet();
    Fragment first = fragmentPersistenceService.get(1);
    fragments.add(first);
    Fragment second = fragmentPersistenceService.get(2);
    fragments.add(second);

    int count = 0;
    for (Fragment got : fragments) {
      assertEquals(datasetKey, got.getDatasetKey());
      assertEquals(crawlId, got.getCrawlId());
      assertTrue(Arrays.equals(abcd206Multi.getBytes(), got.getData()));
      if ("Schistidium agassizii Sull. & Lesq. in Sull.".equals(got.getUnitQualifier())) {
        count++;
      } else if ("Grimmia alpicola Sw. ex Hedw.".equals(got.getUnitQualifier())) {
        count++;
      } else {
        fail("Unexpected UnitQualifier");
      }
    }
    assertEquals(2, count);
  }

  @Test
  public void testUpdateAbcd206OneOfMultiple() {
    // first create new
    UUID datasetKey = UUID.randomUUID();
    OccurrenceSchemaType schemaType = OccurrenceSchemaType.ABCD_2_0_6;
    EndpointType protocol = EndpointType.BIOCASE;
    Integer crawlId = 1;
    fragmentProcessor.buildFragments(datasetKey, abcd206Multi.getBytes(), schemaType, protocol, crawlId, null);

    // now attempt an "update"
    crawlId = 2;
    fragmentProcessor.buildFragments(datasetKey, abcd206MultiModified.getBytes(), schemaType, protocol, crawlId, null);

    Set<Fragment> fragments = Sets.newHashSet();
    Fragment first = fragmentPersistenceService.get(1);
    fragments.add(first);
    Fragment second = fragmentPersistenceService.get(2);
    fragments.add(second);

    int count = 0;
    for (Fragment got : fragments) {
      assertEquals(datasetKey, got.getDatasetKey());
      assertEquals(crawlId, got.getCrawlId());
      assertTrue(Arrays.equals(abcd206MultiModified.getBytes(), got.getData()));
      if ("Schistidium agassizii Sull. & Lesq. in Sull.".equals(got.getUnitQualifier())) {
        count++;
      } else if ("Grimmia alpicola Sw. ex Hedw.".equals(got.getUnitQualifier())) {
        count++;
      } else {
        fail("Unexpected UnitQualifier: " + got.getUnitQualifier());
      }
    }
    assertEquals(2, count);
  }

  @Test
  public void testBadXml() {
    UUID datasetKey = UUID.randomUUID();
    OccurrenceSchemaType schemaType = OccurrenceSchemaType.ABCD_2_0_6;
    Integer crawlId = 1;
    String xml = "<badxml></badxl>";
    // just make sure it doesn't blow up
    try {
      fragmentProcessor.buildFragments(datasetKey, xml.getBytes(), schemaType, EndpointType.BIOCASE, crawlId, null);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testHBaseFailureDuringKeyLookup() {
    // make sure that a keylookup failure due to hbase failure does not result in same behaviour as "key not found"
    occurrenceKeyService = new OccurrenceKeyPersistenceService() {
      @Nullable
      @Override
      public KeyLookupResult findKey(Set<UniqueIdentifier> uniqueIdentifiers) {
        throw new ServiceUnavailableException("connection failure");
      }

      @Override
      public Set<Integer> findKeysByDataset(String datasetKey) {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public KeyLookupResult generateKey(Set<UniqueIdentifier> uniqueIdentifiers) {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public void deleteKey(int occurrenceKey, @Nullable String datasetKey) {
        throw new UnsupportedOperationException("Not implemented yet");
      }

      @Override
      public void deleteKeyByUniqueIdentifiers(Set<UniqueIdentifier> uniqueIdentifiers) {
        throw new UnsupportedOperationException("Not implemented yet");
      }
    };
    fragmentProcessor =
      new FragmentProcessor(fragmentPersistenceService, occurrenceKeyService, messagePublisher, zookeeperConnector);

    UUID datasetKey = UUID.randomUUID();
    OccurrenceSchemaType schemaType = OccurrenceSchemaType.ABCD_2_0_6;
    EndpointType protocol = EndpointType.BIOCASE;
    Integer crawlId = 1;
    fragmentProcessor.buildFragments(datasetKey, abcd206Single.getBytes(), schemaType, protocol, crawlId, null);

    Fragment got = fragmentPersistenceService.get(1);
    assertNull(got);
  }
}
