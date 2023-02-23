package org.gbif.occurrence.download.service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides Download Identifier using the same identifier pre-fix to Apache Oozie.
 */
public class DownloadIdService {

  private final String startTime = new SimpleDateFormat("yyMMddHHmmssSSS").format(new Date());;
  private final AtomicLong counter = new AtomicLong();

  private static String longPadding(long number) {
    StringBuilder sb = new StringBuilder();
    sb.append(number);
    if (sb.length() <= 7) {
      sb.insert(0, "0000000".substring(sb.length()));
    }
    return sb.toString();
  }

  /**
   * Create a unique ID following the format used by Apache Oozie.
   * It is implemented in this way to keep compatibility with the old format.
   */
  public String generateId() {
    return longPadding(counter.getAndIncrement()) + '-' + startTime;
  }
}
