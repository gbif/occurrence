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

import org.gbif.api.model.occurrence.VerbatimOccurrence;

import java.util.List;
import java.util.Map;

/**
 * Second source of the records that aren't in the records table, or of all of them when the table
 * can't be read, e.g. the Elasticsearch _source while the indices keep it.
 *
 * @param <T> interpreted record type
 */
public interface RecordsFallback<T extends VerbatimOccurrence> {

  /** Records of the ids that exist, by id. */
  Map<String, T> get(List<String> ids);

  /** Verbatim records of the ids that exist, by id. */
  Map<String, VerbatimOccurrence> getVerbatim(List<String> ids);
}
