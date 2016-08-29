package org.gbif.occurrence.download.file.common;

import org.gbif.api.vocabulary.License;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Collects dataset records/information used in a occurrence download.
 */
public class DatasetUsagesCollector {

  private Map<UUID, Long> datasetUsages = Maps.newHashMap();

  // we simply keep the String used to identify the licenses to avoid the conversion to License each time
  private Set<String> datasetLicensesString = Sets.newHashSet();
  private Set<License> datasetLicenses = Sets.newHashSet();

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
   * Increments in 1 the number of records coming from the dataset (if any) parameter and record the license.
   *
   * @param datasetKey
   * @param license
   */
  public void incrementDatasetUsage(String datasetKey, String license) {
    incrementDatasetUsage(datasetKey);

    if(license != null && !datasetLicensesString.contains(license)) {
      Optional<License> l = License.fromString(license);
      if(l.isPresent()) {
        datasetLicensesString.add(license);
        datasetLicenses.add(l.get());
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

  public void mergeLicenses(Set<License> licenses){
    // we don't really need to update datasetLicensesString
    datasetLicenses.addAll(licenses);
  }

  /**
   * Dataset usages: number of records used per dataset in download.
   */
  public Map<UUID, Long> getDatasetUsages() {
    return datasetUsages;
  }

  /**
   * Dataset licenses: all distinct licenses used in the download.
   * @return
   */
  public Set<License> getDatasetLicenses(){
    return datasetLicenses;
  }
}
