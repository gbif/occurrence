package org.gbif.occurrence.download.file.specieslist;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.download.hive.DownloadTerms;

/**
 * 
 * Utility class which collects distinct species info and read/write them from file.
 *
 */
public class SpeciesListCollector {

  private final Map<String,Map<String,String>> distinctSpeciesRecord = new HashMap<>();
  
  /**
   * @return list of records of distinct species.
   */
  public Set<Map<String, String>> getDistinctSpecies() {
    return new HashSet<>(distinctSpeciesRecord.values());
  }
  
  /**
   * group results by taxon key and order them in {@link DownloadTerms} species list download order.
   * @return distinct species
   */
  public void computeDistinctSpecies(Map<String, String> filteredResult) {
    String taxonKey = filteredResult.get(GbifTerm.taxonKey.simpleName());

    distinctSpeciesRecord.put(taxonKey, distinctSpeciesRecord.compute(taxonKey, (k, v) -> {
      //if the values are already there increment
      if (v != null) {
        long count = Long.parseLong(v.get(GbifTerm.numOfOccurrences.simpleName())) + 1L;
        v.put(GbifTerm.numOfOccurrences.simpleName(), Long.toString(count));
        return v;
      } 
      else {
        Map<String, String> orderedResults = new LinkedHashMap<>();
        filteredResult.put(GbifTerm.numOfOccurrences.simpleName(), Long.toString(1L));
        // order the results according to download
        DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS.iterator().forEachRemaining(
            term -> orderedResults.put(term.simpleName(), filteredResult.get(term.simpleName())));
        return orderedResults;
      }
    }));
  }
}
