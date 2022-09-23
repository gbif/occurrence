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
package org.gbif.occurrence.download.file.dwca.archive;

import org.gbif.api.model.registry.Dataset;

import java.util.Comparator;
import java.util.UUID;

import lombok.Builder;
import lombok.Data;

/**
 * Simple, local representation for a constituent dataset of download.
 */
@Data
@Builder
public class ConstituentDataset implements Comparable<ConstituentDataset> {

  //Comparator based on number of records and then key
  private static final Comparator<ConstituentDataset> CONSTITUENT_COMPARATOR =
    Comparator.comparingLong(ConstituentDataset::getRecords).thenComparing(ConstituentDataset::getKey);

  private final UUID key;
  private final long records;
  private final Dataset dataset;

  @Override
  public int compareTo(ConstituentDataset other) {
    return CONSTITUENT_COMPARATOR.compare(this, other);
  }

}
