package org.gbif.occurrence.download.file.specieslist;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.download.hive.DownloadTerms;

public class SpeciesListCollector {
  
  private final List<Map<String,String>> distinctSpeciesRecord = new ArrayList<>();
  /**
   * @return list of records of distinct species.
   */
  public List<Map<String, String>> getDistinctSpecies() {
    return distinctSpeciesRecord;
  }
  
  /**
   * group results by taxon key and order them in {@link DownloadTerms} species list download order.
   * @return distinct species
   */
  public SpeciesListCollector computeDistinctSpecies(List<Map<String,String>> filteredResult) {
    if(!distinctSpeciesRecord.isEmpty())
      distinctSpeciesRecord.clear();
    Map<String, List<Map<String, String>>> groupByTaxonKey = filteredResult.stream()
        .collect(Collectors.groupingBy( occMap -> occMap.get(GbifTerm.taxonKey.simpleName())));
    groupByTaxonKey.values().iterator().forEachRemaining(groupedResult -> {
      Map<String, String> orderedResults = new LinkedHashMap<>();
      // takes reference values for the download results
      Map<String, String> referenceResult = new LinkedHashMap<>(groupedResult.get(0));
      referenceResult.put(GbifTerm.numOfOccurrences.simpleName(), Long.toString(groupedResult.size()));
      // order the map according to download terms declaration
      DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS.iterator().forEachRemaining( term -> orderedResults.put(term.simpleName(), referenceResult.get(term.simpleName())));
      distinctSpeciesRecord.add(orderedResults);
    });
    return this;
  }
  
}
