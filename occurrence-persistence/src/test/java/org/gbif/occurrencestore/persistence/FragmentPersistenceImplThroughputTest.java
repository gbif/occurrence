package org.gbif.occurrencestore.persistence;

import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.occurrencestore.common.model.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrencestore.common.model.UniqueIdentifier;
import org.gbif.occurrencestore.persistence.api.Fragment;
import org.gbif.occurrencestore.persistence.api.FragmentPersistenceService;
import org.gbif.occurrencestore.persistence.keygen.HBaseLockingKeyService;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * Note not a real JUnit test, but an extremely expensive performance test that should use the real cluster.
 */
public class FragmentPersistenceImplThroughputTest {

  private static final String LOOKUP_TABLE_NAME = "keygen_test_occurrence_lookup";
  private static final String COUNTER_TABLE_NAME = "keygen_test_occurrence_counter";
  private static final String OCCURRENCE_TABLE_NAME = "keygen_test_occurrence";

  private final FragmentPersistenceServiceImpl fragService;

  private static final AtomicInteger fragsPersisted = new AtomicInteger(0);

  public FragmentPersistenceImplThroughputTest(int hbasePoolSize) {
    HTablePool tablePool = new HTablePool(HBaseConfiguration.create(), hbasePoolSize);
    HBaseLockingKeyService keyService =
      new HBaseLockingKeyService(LOOKUP_TABLE_NAME, COUNTER_TABLE_NAME, OCCURRENCE_TABLE_NAME, tablePool);
    fragService = new FragmentPersistenceServiceImpl(OCCURRENCE_TABLE_NAME, tablePool,
      new OccurrenceKeyPersistenceServiceImpl(keyService));
  }

  public void testNoContention(int threadCount) throws InterruptedException {
    // test generating ids as fast as possible in the ideal case of no waiting for contention (all ids are globally
    // unique)
    int genPerThread = 10000;
    List<Thread> threads = Lists.newArrayList();
    for (int i = 0; i < threadCount; i++) {
      Thread thread = new Thread(new FragmentPersister(fragService, UUID.randomUUID(), genPerThread));
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
                             + " frags/sec] with frag persist time of [" + (threadCount * 1000 / runningAvg)
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

    private final FragmentPersistenceService fragService;
    private final UUID datasetKey;
    private final int genCount;
    private final Fragment fragment;

    private FragmentPersister(FragmentPersistenceService fragService, UUID datasetKey, int genCount) {
      this.fragService = fragService;
      this.datasetKey = datasetKey;
      this.genCount = genCount;
      String xml = "<DarwinRecord>\n"
                   + "  <GlobalUniqueIdentifier>ZMA:Entomology:Diptera_Tipulidae_NL_TEMP_09183</GlobalUniqueIdentifier>\n"
                   + "  <DateLastModified>2007-05-02</DateLastModified>\n"
                   + "  <BasisOfRecord>Museum specimen</BasisOfRecord>\n" + "  <InstitutionCode>ZMA</InstitutionCode>\n"
                   + "  <CollectionCode>Tipulidae</CollectionCode>\n" + "  <CatalogNumber>TEMP_09183</CatalogNumber>\n"
                   + "  <ScientificName>Ctenophora (Cnemoncosis) festiva Meigen, 1804</ScientificName>\n"
                   + "  <Kingdom>Animalia</Kingdom>\n" + "  <Phylum nil=\"true\"/>\n" + "  <Class>Insecta</Class>\n"
                   + "  <Order nil=\"true\"/>\n" + "  <Family>Tipulidae</Family>\n" + "  <Genus>Ctenophora</Genus>\n"
                   + "  <SpecificEpithet>festiva</SpecificEpithet>\n" + "  <InfraspecificEpithet nil=\"true\"/>\n"
                   + "  <Continent nil=\"true\"/>\n" + "  <WaterBody nil=\"true\"/>\n"
                   + "  <Country>Netherlands</Country>\n" + "  <StateProvince>Utrecht</StateProvince>\n"
                   + "  <Locality>Leusden</Locality>\n" + "  <MinimumElevationInMeters nil=\"true\"/>\n"
                   + "  <MaximumElevationInMeters nil=\"true\"/>\n" + "  <MinimumDepthInMeters nil=\"true\"/>\n"
                   + "  <MaximumDepthInMeters nil=\"true\"/>\n" + "  <CollectingMethod>unrecorded</CollectingMethod>\n"
                   + "  <EarliestDateCollected>1985-06-08 00:00:00</EarliestDateCollected>\n"
                   + "  <LatestDateCollected>1985-06-08 00:00:00</LatestDateCollected>\n"
                   + "  <DayOfYear nil=\"true\"/>\n" + "  <Collector>Zeegers, T.</Collector>\n"
                   + "  <DecimalLatitude nil=\"true\"/>\n" + "  <DecimalLongitude nil=\"true\"/>\n"
                   + "  <CoordinateUncertaintyInMeters nil=\"true\"/>\n" + "  <Preparations>dry pinned</Preparations>\n"
                   + "  <TypeStatus nil=\"true\"/>\n" + "</DarwinRecord>\n";
      byte[] hash = DigestUtils.md5(xml);
      fragment =
        new Fragment(datasetKey, xml.getBytes(Charsets.UTF_8), hash, Fragment.FragmentType.XML, EndpointType.DIGIR,
          new Date(), 1, OccurrenceSchemaType.DWC_MANIS, null, System.currentTimeMillis());
    }

    @Override
    public void run() {
      for (int i = 0; i < genCount; i++) {
        UniqueIdentifier unique = new PublisherProvidedUniqueIdentifier(datasetKey, String.valueOf(i));
        fragService.insert(fragment, ImmutableSet.of(unique));
        //        fragment.setKey(i);
        //        fragService.update(fragment);
        fragsPersisted.incrementAndGet();
      }
    }
  }

  public static void main(String[] args) throws InterruptedException {
    // following stats from single regionserver, single region
    // on pure insert, raw rate is ~3k/sec
    // on pure update where some fields null, raw rate is ~8k/sec
    // on pure update where no fields null, raw rate is ~15k/sec

    int hbasePoolSize = 100;
    int persistingThreads = 100;
    if (args.length == 2) {
      hbasePoolSize = Integer.valueOf(args[0]);
      persistingThreads = Integer.valueOf(args[1]);
    }
    System.out
      .println("Running test with hbasePool [" + hbasePoolSize + "] and persistingThreads [" + persistingThreads + "]");
    FragmentPersistenceImplThroughputTest instance = new FragmentPersistenceImplThroughputTest(hbasePoolSize);
    instance.testNoContention(persistingThreads);
  }
}
