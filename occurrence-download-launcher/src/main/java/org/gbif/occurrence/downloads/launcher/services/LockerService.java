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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LockerService {

  private final Map<String, Thread> lockMap = new ConcurrentHashMap<>();

  public void lock(String id, Thread thread) {
    log.info("Lock the thread for id {}", id);
    lockMap.put(id, thread);
    LockSupport.park(thread);
  }

  public void unlock(String id) {
    log.info("Unlock the thread for id {}", id);
    Thread thread = lockMap.get(id);
    if (thread != null) {
      LockSupport.unpark(thread);
      lockMap.remove(id);
      log.info("The thread for id {} is unlocked", id);
    }
  }

  public void unlockAll() {
    log.info("Unlock all threads");
    if (!lockMap.isEmpty()) {

      lockMap.forEach(
          (id, thread) -> {
            log.info("Unpark thread for id {}", id);
            LockSupport.unpark(thread);
          });

      lockMap.clear();
    }
  }
}
