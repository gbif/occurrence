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
package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.occurrence.search.OccurrenceGetByKey;

import java.util.UUID;

/**
 * Adapts {@link OccurrenceWsSearchClient} to {@link OccurrenceGetByKey} so occurrence-ws
 * can use the search-ws get-by-key endpoints (fragment by datasetKey/occurrenceId, Annosys) without
 * depending on ES directly.
 */
public class OccurrenceGetByKeyClientAdapter implements OccurrenceGetByKey {

  private final OccurrenceWsSearchClient client;

  public OccurrenceGetByKeyClientAdapter(OccurrenceWsSearchClient client) {
    this.client = client;
  }

  @Override
  public Occurrence get(Long key) {
    return client.get(key);
  }

  @Override
  public VerbatimOccurrence getVerbatim(Long key) {
    return client.getVerbatim(key);
  }

  @Override
  public Occurrence get(UUID datasetKey, String occurrenceId) {
    return client.get(datasetKey, occurrenceId);
  }

  @Override
  public VerbatimOccurrence getVerbatim(UUID datasetKey, String occurrenceId) {
    return client.getVerbatim(datasetKey, occurrenceId);
  }
}
