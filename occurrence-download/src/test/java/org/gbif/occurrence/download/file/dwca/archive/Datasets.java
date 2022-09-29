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

import org.gbif.api.model.registry.Citation;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.License;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import lombok.experimental.UtilityClass;

/**
 * Test class to generate test datasets.
 */
@UtilityClass
public class Datasets {

  private static final Random RANDOM_LICENSE = new Random();

  /**
   * Creates a test dataset, the idx is concatenated to the title and citation text.
   * The license is randomly generated between CC0_1_0, CC_BY_4_0 and CC_BY_NC_4_0.
   */
  private static Dataset newDataset(int idx) {
    Citation citation = new Citation();
    citation.setText("Citation " + idx);
    Dataset dataset = new Dataset();
    dataset.setKey(UUID.randomUUID());
    dataset.setTitle("Dataset " + idx);
    dataset.setLicense(License.values()[RANDOM_LICENSE.nextInt(2)]);
    dataset.setCitation(citation);
    return dataset;
  }

  /**
   * Creates a number of random test datasets with title and license.
   */
  public static List<Dataset> testDatasets(int size) {

    return  IntStream.range(0, size)
      .boxed()
      .map(Datasets::newDataset)
      .collect(Collectors.toList());
  }

  /**
   * Creates a test constituent dataset, use the a test dataset.
   */
  private ConstituentDataset newConstituentDataset(int idx) {
    Dataset dataset = newDataset(idx);
    return ConstituentDataset.builder()
      .dataset(dataset)
      .key(dataset.getKey())
      .records(idx + 1)
      .build();
  }

  /**
   * Creates a test list of constituents datasets.
   */
  public static List<ConstituentDataset> testConstituentsDatasets(int size) {
    return IntStream.range(0, size)
             .boxed()
             .map(Datasets::newConstituentDataset)
             .collect(Collectors.toList());
  }

  /**
   * Creates a maps of dataset keys and usages/counts.
   * The count is the index number.
   */
  public static Map<UUID,Long> testUsages(int numberOfRecords) {
    HashMap<UUID,Long> usages = new HashMap<>();
    LongStream.rangeClosed(1, numberOfRecords + 1)
      .boxed()
      .forEach(count -> usages.put(UUID.randomUUID(), count));
    return usages;
  }
}
