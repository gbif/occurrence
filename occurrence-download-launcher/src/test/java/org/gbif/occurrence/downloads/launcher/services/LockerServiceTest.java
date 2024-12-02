/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.downloads.launcher.services;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

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
