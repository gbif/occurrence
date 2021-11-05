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
package org.gbif.occurrence.persistence.experimental;

import java.util.List;

/**
 * Provides the assertions for an occurrence linking it to similar records.
 * If this proves useful it will be merged in to the OccurrenceService interface.
 */
public interface OccurrenceRelationshipService {

  /**
   * Provides the occurrences that relate to the given key.
   * @param key The record key for which we seek related occurrences
   * @return A list of related occurrences in the structure stored in the table (a JSON String)
   */
  List<String> getRelatedOccurrences(long key);

  /**
   * Provides the cached view of the "current" occurrence within the relationship (it may be stale compared to live
   * data).
   * @param key The record key for which we seek related occurrences
   * @return A JSON String for the current occurrence
   */
  String getCurrentOccurrence(long key);
}
