package org.gbif.occurrence.download.file.common;

import org.gbif.dwc.terms.GbifTerm;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Maps;

public class DatasetUsagesCollector {

  private Map<UUID, Long> datasetUsages = new HashMap();

  /**
   * Increments in 1 the number of records coming from the dataset (if any) in the occurrencRecordMap.
   */
  public void collectUsage(Map<String, String> occurrenceRecordMap) {
    final String datasetStrKey = occurrenceRecordMap.get(GbifTerm.datasetKey.simpleName());
    if (datasetStrKey != null) {
      UUID datasetKey = UUID.fromString(datasetStrKey);
      if (datasetUsages.containsKey(datasetKey)) {
        datasetUsages.put(datasetKey, datasetUsages.get(datasetKey) + 1);
      } else {
        datasetUsages.put(datasetKey, 1L);
      }
    }
  }

  public void sumUsages(Map<UUID, Long> fromDatasetUsages){
    Map<UUID, Long> result = Maps.newHashMap();
    for (Map.Entry<UUID, Long> entry1 : datasetUsages.entrySet()) {
      Long valueIn2 = fromDatasetUsages.get(entry1.getKey());
      if (valueIn2 == null) {
        result.put(entry1.getKey(), entry1.getValue());
      } else {
        result.put(entry1.getKey(), entry1.getValue() + valueIn2);
      }
    }
    result.putAll(Maps.difference(datasetUsages, fromDatasetUsages).entriesOnlyOnRight());
    datasetUsages = result;
  }

  public Map<UUID, Long> getDatasetUsages(){
    return datasetUsages;
  }
}
