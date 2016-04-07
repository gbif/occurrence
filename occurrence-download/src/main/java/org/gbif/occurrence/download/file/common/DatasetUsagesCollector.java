package org.gbif.occurrence.download.file.common;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Maps;

/**
 * Collects dataset records used in a occurrence download.
 */
public class DatasetUsagesCollector {

  private Map<UUID, Long> datasetUsages = new HashMap();

  /**
   * Increments in 1 the number of records coming from the dataset (if any) parameter.
   */
  public void incrementDatasetUsage(String datasetKey) {
    if (datasetKey != null) {
      UUID datasetUUID = UUID.fromString(datasetKey);
      if (datasetUsages.containsKey(datasetUUID)) {
        datasetUsages.put(datasetUUID, datasetUsages.get(datasetUUID) + 1);
      } else {
        datasetUsages.put(datasetUUID, 1L);
      }
    }
  }

  /**
   * Sums all the dataset usages to current instance.
   */
  public void sumUsages(Map<UUID, Long> fromDatasetUsages) {
    Map<UUID, Long> result = Maps.newHashMap();
    for (Map.Entry<UUID, Long> entry : datasetUsages.entrySet()) {
      Long valueIn2 = fromDatasetUsages.get(entry.getKey());
      if (valueIn2 == null) {
        result.put(entry.getKey(), entry.getValue());
      } else {
        result.put(entry.getKey(), entry.getValue() + valueIn2);
      }
    }
    result.putAll(Maps.difference(datasetUsages, fromDatasetUsages).entriesOnlyOnRight());
    datasetUsages = result;
  }

  /**
   * Dataset usages: number of records used per dataset in download..
   */
  public Map<UUID, Long> getDatasetUsages() {
    return datasetUsages;
  }
}
