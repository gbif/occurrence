package org.gbif.occurrence.downloads.launcher.services;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LockerServiceTest {

  private LockerService lockerService;
  private ExecutorService executorService;

  @BeforeEach
  public void setup() {
    lockerService = new LockerService();
    executorService = Executors.newFixedThreadPool(2);
  }

  @Test
  void testLockAndUnlock() throws InterruptedException {
    String id = "TestThread";

    executorService.submit(
        () -> {
          lockerService.lock(id, Thread.currentThread());
          assertFalse(
              Thread.currentThread().isInterrupted(), "Thread should be unparked after unlock");
        });

    // Allow the thread to be parked
    TimeUnit.SECONDS.sleep(1);

    lockerService.unlock(id);

    // Allow the thread to unpark
    TimeUnit.SECONDS.sleep(1);

    // Shutdown the executor service
    executorService.shutdown();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  void testUnlockAll() throws InterruptedException {
    String id1 = "TestThread1";
    String id2 = "TestThread2";

    executorService.submit(
        () -> {
          lockerService.lock(id1, Thread.currentThread());
          assertFalse(
              Thread.currentThread().isInterrupted(), "Thread should be unparked after unlockAll");
        });

    executorService.submit(
        () -> {
          lockerService.lock(id2, Thread.currentThread());
          assertFalse(
              Thread.currentThread().isInterrupted(), "Thread should be unparked after unlockAll");
        });

    // Allow the threads to be parked
    TimeUnit.SECONDS.sleep(1);

    lockerService.unlockAll();

    // Allow the threads to unpark
    TimeUnit.SECONDS.sleep(1);

    // Shutdown the executor service
    executorService.shutdown();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }
}
