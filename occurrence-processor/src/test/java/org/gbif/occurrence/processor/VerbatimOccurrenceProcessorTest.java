package org.gbif.occurrence.processor;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.FragmentPersistedMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.processor.messaging.FragmentPersistedListener;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.io.IOException;
import java.util.Date;
import java.util.Set;
import java.util.UUID;

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.test.TestingServer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.gbif.api.model.occurrence.OccurrencePersistenceStatus.NEW;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Ignore("requires real messaging")
public class VerbatimOccurrenceProcessorTest {

  // BoGART from BGBM
  private final UUID DATASET_KEY = UUID.fromString("85697f04-f762-11e1-a439-00145eb45e9a");

  private static MessagePublisher messagePublisher;
  private static final ConnectionParameters CONNECTION_PARAMETERS =
    new ConnectionParameters("localhost", 5672, "guest", "guest", "/");
  private TestingServer zkServer;
  private CuratorFramework curator;
  private ZookeeperConnector zookeeperConnector;
  private OccurrencePersistenceService occurrenceService;
  private FragmentPersistenceService fragmentService;
  private VerbatimProcessor verbatimProcessor;

  @BeforeClass
  public static void preClass() throws IOException {
    messagePublisher = new DefaultMessagePublisher(CONNECTION_PARAMETERS);
  }

  @Before
  public void setUp() throws Exception {
    long now = new Date().getTime();
    zkServer = new TestingServer();
    curator = CuratorFrameworkFactory.builder().connectString(zkServer.getConnectString()).namespace("crawler")
      .retryPolicy(new RetryNTimes(1, 1000)).build();
    curator.start();
    zookeeperConnector = new ZookeeperConnector(curator);

    fragmentService = new FragmentPersistenceServiceMock(new OccurrenceKeyPersistenceServiceMock());
    String json = Resources.toString(Resources.getResource("fragment.json"), org.apache.commons.io.Charsets.UTF_8);
    Fragment fragment = new Fragment(DATASET_KEY, json.getBytes(Charsets.UTF_8), json.getBytes(Charsets.UTF_8),
      Fragment.FragmentType.JSON, EndpointType.DWC_ARCHIVE, new Date(), 1, null, null, new Date().getTime());
    Set<UniqueIdentifier> uniqueIds = Sets.newHashSet();
    uniqueIds.add(new HolyTriplet(DATASET_KEY, "ic", "cc", "cn", null));
    fragmentService.insert(fragment, uniqueIds);
    occurrenceService = new OccurrencePersistenceServiceMock(fragmentService);
    verbatimProcessor = new VerbatimProcessor(fragmentService, occurrenceService, messagePublisher, zookeeperConnector);
    FragmentPersistedListener fragmentPersistedListener = new FragmentPersistedListener(verbatimProcessor);
    MessageListener messageListener = new MessageListener(CONNECTION_PARAMETERS);
    messageListener.listen("frag_persisted_test_" + now, 1, fragmentPersistedListener);
  }

  @Test
  public void generateVerbFromFrag() throws IOException, InterruptedException {
    messagePublisher
      .send(new FragmentPersistedMessage(UUID.fromString("85697f04-f762-11e1-a439-00145eb45e9a"), 1, NEW, 1));
    Thread.sleep(5000);
    VerbatimOccurrence verb = occurrenceService.getVerbatim(1);
    assertNotNull(verb);
    assertEquals(24, verb.getFields().size());
    assertEquals("Pontaurus", verb.getField(DwcTerm.collectionCode));
  }
}
