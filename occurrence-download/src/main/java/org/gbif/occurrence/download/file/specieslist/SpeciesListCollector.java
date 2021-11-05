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
package org.gbif.occurrence.download.file.specieslist;

import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.download.hive.DownloadTerms;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Utility class which collects distinct species info.
 */
public class SpeciesListCollector {

  private final Map<String, Map<String, String>> distinctSpeciesRecord = new HashMap<>();

  /**
   * @return set of records of distinct species.
   */
  public Set<Map<String, String>> getDistinctSpecies() {
    return new HashSet<>(distinctSpeciesRecord.values());
  }

  /**
   * Gets the species record by taxon key.
   */
  public Map<String, String> getByTaxonKey(String taxonKey) {
    return distinctSpeciesRecord.get(taxonKey);
  }

  /**
   * group results by taxon key and order them in {@link DownloadTerms} species list download order.
   */
  public void collect(Map<String, String> occurrenceRecord) {
    String taxonKey = occurrenceRecord.get(GbifTerm.taxonKey.simpleName());

    distinctSpeciesRecord.put(taxonKey, distinctSpeciesRecord.compute(taxonKey, (k, v) -> {
      //if the values are already there increment
      if (v != null) {
        long count = Long.parseLong(v.get(GbifTerm.numberOfOccurrences.simpleName()))
                     + Long.parseLong(occurrenceRecord.getOrDefault(GbifTerm.numberOfOccurrences.simpleName(), "1"));
        v.put(GbifTerm.numberOfOccurrences.simpleName(), Long.toString(count));
        return v;
      } else {
        occurrenceRecord.putIfAbsent(GbifTerm.numberOfOccurrences.simpleName(), Long.toString(1L));
        // order the results according to download
        return new LinkedHashMap<>(DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS.stream()
                                     .collect(LinkedHashMap::new, (m, val) -> {
                                       m.put(val.getRight().simpleName(), occurrenceRecord.get(val.getRight().simpleName()));
                                     }, LinkedHashMap::putAll));
      }
    }));
  }
}
