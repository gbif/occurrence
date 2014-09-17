package org.gbif.occurrence.persistence.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZookeeperLockManagerTest {

  private static TestingServer SERVER;
  private static CuratorFramework CURATOR;
  private static ZookeeperLockManager ZOO_LOCK_MGR;

  @BeforeClass
  public static void setUp() throws Exception {
    SERVER = new TestingServer();
    CURATOR = CuratorFrameworkFactory.builder().namespace("hbasePersistence").connectString(SERVER.getConnectString())
      .retryPolicy(new RetryNTimes(1, 1000)).build();
    CURATOR.start();
    ZOO_LOCK_MGR = new ZookeeperLockManager(CURATOR);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    CURATOR.close();
    SERVER.stop();
  }

  @Test
  public void testAcquireSuccess() {
    UUID uuid = UUID.randomUUID();
    assertTrue(ZOO_LOCK_MGR.getLock(uuid.toString()));
    ZOO_LOCK_MGR.releaseLock(uuid.toString());
  }

  @Test
  public void testAcquireFail() {
    UUID uuid = UUID.randomUUID();
    assertTrue(ZOO_LOCK_MGR.getLock(uuid.toString()));
    assertFalse(ZOO_LOCK_MGR.getLock(uuid.toString()));
    ZOO_LOCK_MGR.releaseLock(uuid.toString());
  }

  @Test
  public void testAcquireRelease() {
    UUID uuid = UUID.randomUUID();
    assertTrue(ZOO_LOCK_MGR.getLock(uuid.toString()));
    ZOO_LOCK_MGR.releaseLock(uuid.toString());
    assertTrue(ZOO_LOCK_MGR.getLock(uuid.toString()));
    ZOO_LOCK_MGR.releaseLock(uuid.toString());
  }

  @Test
  public void testMultiThreadCompete() throws ExecutionException, InterruptedException {
    UUID uuid = UUID.randomUUID();
    assertTrue(ZOO_LOCK_MGR.getLock(uuid.toString()));
    List<Future<Boolean>> futures = Lists.newArrayList();
    for (int i=0; i < 20; i++) {
      FutureTask<Boolean> future = new FutureTask<Boolean>(new LockGrabber(uuid));
      future.run();
      futures.add(future);
    }
    for (Future<Boolean> future : futures) {
      assertFalse(future.get());
    }
    ZOO_LOCK_MGR.releaseLock(uuid.toString());
  }

  private class LockGrabber implements Callable<Boolean> {
    private UUID uuid;

    private LockGrabber(UUID uuid) {
      this.uuid = uuid;
    }

    @Override
    public Boolean call() throws Exception {
      return ZOO_LOCK_MGR.getLock(uuid.toString());
    }
  }
}
