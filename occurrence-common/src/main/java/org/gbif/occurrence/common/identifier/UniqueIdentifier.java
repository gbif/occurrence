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
package org.gbif.occurrence.common.identifier;

import java.util.UUID;

/**
 * This interface is meant to be used for classes that can uniquely identify occurrence records.
 */
public interface UniqueIdentifier {

  /**
   * Every unique identifier must be scoped within the dataset key.
   *
   * @return the UUID of the dataset for the identified occurrence
   */
  public UUID getDatasetKey();

  /**
   * A string that uniquely identifies the occurrence (e.g. a concatenation of it's unique fields) that could be used
   * as a key for maps or databases.
   *
   * @return a unique String representing the unique identifier
   */
  public String getUniqueString();

  /**
   * A string that uniquely identifies the occurrence within a dataset (e.g. a concatenation of it's unique fields) but
   * does not incorporate the datasetKey.
   *
   * @return a unique String representing the unique identifier within the dataset (but does not include the datasetKey)
   */
  public String getUnscopedUniqueString();
}
