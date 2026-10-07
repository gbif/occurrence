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

import java.util.List;

/** Store of the JSON views of records, identified by the ids of their Elasticsearch documents. */
public interface RecordsStore {

  /** JSON views of a record, null when not requested. */
  record Row(String id, String interpreted, String verbatim) {}

  /**
   * Gets the records of the ids, keeping their order and skipping the ones that don't exist: a
   * record can be deleted after a search returned its id.
   */
  List<Row> get(List<String> ids, boolean interpreted, boolean verbatim);
}
