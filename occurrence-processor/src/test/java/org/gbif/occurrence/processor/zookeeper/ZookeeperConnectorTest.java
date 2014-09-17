package org.gbif.occurrence.processor.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ZookeeperConnectorTest {

  private TestingServer server;
  private CuratorFramework curator;
  private ZookeeperConnector connector;

  @Before
  public void setUp() throws Exception {
    server = new TestingServer();
    curator = CuratorFrameworkFactory.builder().namespace("crawlertest").connectString(server.getConnectString())
      .retryPolicy(new RetryNTimes(1, 1000)).build();
    curator.start();
    connector = new ZookeeperConnector(curator);
  }

  @After
  public void tearDown() throws IOException {
    curator.close();
    server.stop();
  }

  @Test
  @Ignore
  public void testAddCounter() throws Exception {
    UUID datasetKey = UUID.randomUUID();

    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_PROCESSED);
    Long test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_PROCESSED);
    assertEquals(1l, test.longValue());
    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_PROCESSED);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_PROCESSED);
    assertEquals(2l, test.longValue());

    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_RECEIVED);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_RECEIVED);
    assertEquals(1l, test.longValue());
    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_RECEIVED);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_RECEIVED);
    assertEquals(2l, test.longValue());

    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_ERROR);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_ERROR);
    assertEquals(1l, test.longValue());
    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_ERROR);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_ERROR);
    assertEquals(2l, test.longValue());

    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_SUCCESS);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_SUCCESS);
    assertEquals(1l, test.longValue());
    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_SUCCESS);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_SUCCESS);
    assertEquals(2l, test.longValue());

    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_ERROR);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_ERROR);
    assertEquals(1l, test.longValue());
    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_ERROR);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_ERROR);
    assertEquals(2l, test.longValue());

    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_NEW);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_NEW);
    assertEquals(1l, test.longValue());
    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_NEW);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_NEW);
    assertEquals(2l, test.longValue());

    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UPDATED);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UPDATED);
    assertEquals(1l, test.longValue());
    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UPDATED);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UPDATED);
    assertEquals(2l, test.longValue());

    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UNCHANGED);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UNCHANGED);
    assertEquals(1l, test.longValue());
    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UNCHANGED);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UNCHANGED);
    assertEquals(2l, test.longValue());

    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_ERROR);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_ERROR);
    assertEquals(1l, test.longValue());
    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_ERROR);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_ERROR);
    assertEquals(2l, test.longValue());

    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_SUCCESS);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_SUCCESS);
    assertEquals(1l, test.longValue());
    connector.addCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_SUCCESS);
    test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_SUCCESS);
    assertEquals(2l, test.longValue());
  }

  @Test
  @Ignore
  public void testMultiThreadedAdd() throws InterruptedException {
    UUID datasetKey = UUID.randomUUID();

    List<Thread> threads = Lists.newArrayList();
    for (int i = 0; i < 20; i++) {
      Thread thread = new CountAddingThread(connector, ZookeeperConnector.CounterName.FRAGMENT_RECEIVED, datasetKey, 1);
      thread.start();
      threads.add(thread);
    }
    for (Thread thread : threads) {
      thread.join();
    }

    Long test = connector.readCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_RECEIVED);
    assertEquals(20l, test.longValue());
  }

  @Test
  @Ignore("too expensive")
  public void testPerformance() throws InterruptedException {
    // add 1M counts on 100 threads for the same counter
    UUID datasetKey = UUID.randomUUID();

    Stopwatch stopwatch = new Stopwatch().start();
    int totalCounts = 100000;
    int threadCount = 100;
    ExecutorService tp = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      tp.execute(new CountAddingThread(connector, ZookeeperConnector.CounterName.FRAGMENT_RECEIVED, datasetKey,
        totalCounts / threadCount));
    }
    tp.shutdown();
    tp.awaitTermination(10, TimeUnit.MINUTES);
    stopwatch.stop();
    System.out.println(
      "zk counter adds took [" + stopwatch + "] for an average of [" + stopwatch.elapsed(TimeUnit.MILLISECONDS) / totalCounts
      + " ms/write]");
    fail();
  }

  private static class CountAddingThread extends Thread {

    private final ZookeeperConnector zkConnector;
    private final ZookeeperConnector.CounterName counterName;
    private final UUID uuid;
    private final int count;

    private CountAddingThread(ZookeeperConnector zkConnector, ZookeeperConnector.CounterName counterName, UUID uuid,
      int count) {
      this.zkConnector = zkConnector;
      this.counterName = counterName;
      this.uuid = uuid;
      this.count = count;
    }

    @Override
    public void run() {
      for (int i = 0; i < count; i++) {
        zkConnector.addCounter(uuid, counterName);
      }
    }
  }
}
