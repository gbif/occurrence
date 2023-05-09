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
