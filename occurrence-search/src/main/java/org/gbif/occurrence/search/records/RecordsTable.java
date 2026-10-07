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
package org.gbif.occurrence.search.records;

/**
 * Row keys and columns of the HBase records tables written by pipelines (see
 * docs/hbase-records-tables.md and RecordsTableKey in gbif/pipelines).
 *
 * <ul>
 *   <li>Occurrences: {@code <gbifId % 100, 2 digits>:<gbifId>}, e.g. "67:1234567".
 *   <li>Events: the internalId, which is also the id of the event documents in Elasticsearch.
 * </ul>
 */
public final class RecordsTable {

  public static final String COLUMN_FAMILY = "o";
  /** JSON of the API Occurrence (or Event). */
  public static final String INTERPRETED_COLUMN = "interpreted";
  /** JSON of the API VerbatimOccurrence. */
  public static final String VERBATIM_COLUMN = "verbatim";

  private static final int OCCURRENCE_SALT = 100;

  private RecordsTable() {}

  public static String occurrenceRowKey(long gbifId) {
    return String.format("%02d:%d", gbifId % OCCURRENCE_SALT, gbifId);
  }

  /** @param gbifId occurrence key, as used for the id of the Elasticsearch documents */
  public static String occurrenceRowKey(String gbifId) {
    return occurrenceRowKey(Long.parseLong(gbifId));
  }

  public static String eventRowKey(String internalId) {
    return internalId;
  }
}
