package org.gbif.occurrence.processor;

import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.persistence.FragmentPersistenceServiceImpl;
import org.gbif.occurrence.persistence.OccurrenceKeyPersistenceServiceImpl;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * Note not a real JUnit test but rather an extremely expensive test that is meant to be run against the real cluster.
 */
public class FragmentProcessorThroughputTest {

  private static final String LOOKUP_TABLE_NAME = "keygen_test_occurrence_lookup";
  private static final String COUNTER_TABLE_NAME = "keygen_test_occurrence_counter";
  private static final String OCCURRENCE_TABLE_NAME = "keygen_test_occurrence";

  private static final String xmlPrefix =
    "<DarwinRecord><GlobalUniqueIdentifier>ZMA:Entomology:Diptera_Tipulidae_NL_TEMP_09183</GlobalUniqueIdentifier>"
    + "<DateLastModified>2007-05-02</DateLastModified><BasisOfRecord>Museum specimen</BasisOfRecord>"
    + "<InstitutionCode>ZMA</InstitutionCode><CollectionCode>Tipulidae</CollectionCode><CatalogNumber>";
  private static final String xmlSuffix =
    "</CatalogNumber><ScientificName>Ctenophora (Cnemoncosis) festiva Meigen, 1804</ScientificName>"
    + "<Kingdom>Animalia</Kingdom><Phylum nil=\"true\"/><Class>Insecta</Class>"
    + "<Order nil=\"true\"/><Family>Tipulidae</Family><Genus>Ctenophora</Genus>"
    + "<SpecificEpithet>festiva</SpecificEpithet>" + "<InfraspecificEpithet nil=\"true\"/><Continent nil=\"true\"/>"
    + "<WaterBody nil=\"true\"/>" + "<Country>Netherlands</Country>"
    + "<StateProvince>Utrecht</StateProvince><Locality>Leusden</Locality><MinimumElevationInMeters nil=\"true\"/>"
    + "<MaximumElevationInMeters nil=\"true\"/><MinimumDepthInMeters nil=\"true\"/>"
    + "<MaximumDepthInMeters nil=\"true\"/><CollectingMethod>unrecorded</CollectingMethod>"
    + "<EarliestDateCollected>1985-06-08 00:00:00</EarliestDateCollected>"
    + "<LatestDateCollected>1985-06-08 00:00:00</LatestDateCollected><DayOfYear nil=\"true\"/>"
    + "<Collector>Zeegers, T.</Collector><DecimalLatitude nil=\"true\"/><DecimalLongitude nil=\"true\"/>"
    + "<CoordinateUncertaintyInMeters nil=\"true\"/><Preparations>dry pinned</Preparations>"
    + "<TypeStatus nil=\"true\"/></DarwinRecord>";

  private final FragmentProcessor fragmentProcessor;

  private static final AtomicInteger fragsPersisted = new AtomicInteger(0);

  public FragmentProcessorThroughputTest(int hbasePoolSize) throws IOException {
    HTablePool tablePool = new HTablePool(HBaseConfiguration.create(), hbasePoolSize);
    HBaseLockingKeyService keyService =
      new HBaseLockingKeyService(LOOKUP_TABLE_NAME, COUNTER_TABLE_NAME, OCCURRENCE_TABLE_NAME, tablePool);
    OccurrenceKeyPersistenceService occurrenceKeyService = new OccurrenceKeyPersistenceServiceImpl(keyService);
    FragmentPersistenceService fragService =
      new FragmentPersistenceServiceImpl(OCCURRENCE_TABLE_NAME, tablePool, occurrenceKeyService);
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost("mq.gbif.org");
    connectionFactory.setVirtualHost("/users/omeyn");
    connectionFactory.setUsername("omeyn");
    connectionFactory.setPassword("omeyn");
    MessagePublisher messagePublisher =
      new DefaultMessagePublisher(new ConnectionParameters("mq.gbif.org", 5672, "omeyn", "omeyn", "/users/omeyn"));
    CuratorFramework curator =
      CuratorFrameworkFactory.builder().namespace("/fragproctest").retryPolicy(new ExponentialBackoffRetry(25, 100))
        .connectString("c1n1.gbif.org:2181").build();
    curator.start();
    ZookeeperConnector zkConnector = new ZookeeperConnector(curator);
    fragmentProcessor = new FragmentProcessor(fragService, occurrenceKeyService, messagePublisher, zkConnector);
  }

  public void testNoContention(int threadCount) throws InterruptedException {
    // test generating ids as fast as possible in the ideal case of no waiting for contention (all ids are globally
    // unique)
    int genPerThread = 10000;
    List<Thread> threads = Lists.newArrayList();
    for (int i = 0; i < threadCount; i++) {
      Thread thread = new Thread(new FragmentPersister(fragmentProcessor, UUID.randomUUID(), genPerThread));
      thread.start();
      threads.add(thread);
    }

    Thread rateReporter = new Thread(new RateReporter(threadCount));
    rateReporter.start();

    for (Thread thread : threads) {
      thread.join();
    }

    rateReporter.interrupt();
    rateReporter.join();
  }

  private static class RateReporter implements Runnable {

    private final int threadCount;

    private RateReporter(int threadCount) {
      this.threadCount = threadCount;
    }

    @Override
    public void run() {
      int periods = 0;
      int runningAvg = 0;
      int buildAverageAfter = 15;
      int lastCount = 0;
      boolean interrupted = false;
      while (!interrupted) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          interrupted = true;
        }
        int generated = fragsPersisted.intValue() - lastCount;
        if (periods > buildAverageAfter) {
          if (runningAvg == 0) {
            runningAvg = generated;
          } else {
            int netPeriods = periods - buildAverageAfter;
            runningAvg = (netPeriods * runningAvg + generated) / (netPeriods + 1);
          }
          System.out.println("Frags persisted at [" + generated + " frags/s] for running avg of [" + runningAvg
                             + " frags/s] and per thread [" + (runningAvg / threadCount)
                             + " frags/sec] with frag process time of [" + (threadCount * 1000 / runningAvg)
                             + " ms/frag]");
        } else {
          System.out.println("Stats in [" + (buildAverageAfter - periods) + "] seconds.");
        }
        periods++;
        lastCount = fragsPersisted.intValue();
      }
    }
  }

  private static class FragmentPersister implements Runnable {

    private final FragmentProcessor fragProcessor;
    private final UUID datasetKey;
    private final OccurrenceSchemaType schema = OccurrenceSchemaType.DWC_MANIS;
    private final int genCount;

    private FragmentPersister(FragmentProcessor fragProcessor, UUID datasetKey, int genCount) {
      this.fragProcessor = fragProcessor;
      this.datasetKey = datasetKey;
      this.genCount = genCount;
    }

    @Override
    public void run() {
      for (int i = 0; i < genCount; i++) {
        String xml = xmlPrefix + i + xmlSuffix;
        fragProcessor.buildFragments(datasetKey, xml.getBytes(Charsets.UTF_8), schema, EndpointType.BIOCASE, 1, null);
        fragsPersisted.incrementAndGet();
      }
    }
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    // against c4 runs around 3-3.3k/sec - appears limited by persistence
    int hbasePoolSize = 20;
    int persistingThreads = 20;
    if (args.length == 2) {
      hbasePoolSize = Integer.valueOf(args[0]);
      persistingThreads = Integer.valueOf(args[1]);
    }
    System.out
      .println("Running test with hbasePool [" + hbasePoolSize + "] and persistingThreads [" + persistingThreads + "]");
    FragmentProcessorThroughputTest instance = new FragmentProcessorThroughputTest(hbasePoolSize);
    instance.testNoContention(persistingThreads);
  }
}
