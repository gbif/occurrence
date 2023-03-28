package org.gbif.occurrence.downloads.launcher.services;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class LockerService {

  private final Map<String, Thread> lockMap = new ConcurrentHashMap<>();

  public void lock(String id, Thread thread) {
    log.info("Lock the thread for {}", id);
    lockMap.put(id, thread);
    LockSupport.park(thread);
  }

  public void unlock(String id) {
    log.info("Unlock the thread for {}", id);
    Thread thread = lockMap.get(id);
    if (thread != null) {
      LockSupport.unpark(thread);
      lockMap.remove(id);
      log.info("The thread for {} is unlocked", id);
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
