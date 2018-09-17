package org.gbif.occurrence.download.file.specieslist;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.download.hive.DownloadTerms;

public enum SpeciesListCollector {
  INSTANCE;  
  
  public static SpeciesListCollector getInstance() {
    return INSTANCE;
  }
  
  private List<Map<String,String>> collectedResults = new ArrayList<>();
  
  public synchronized void collectResult(Map<String,String> result) {
    collectedResults.add(result);    
  }
  
  public List<Map<String,String>> groupByTaxonKey() {
    Map<String,List<Map<String,String>>> groupByTaxonKey = collectedResults.stream().collect(Collectors.groupingBy((occMap) -> occMap.get(GbifTerm.taxonKey.simpleName())));
    List<Map<String,String>> results = new ArrayList<>();
    groupByTaxonKey.values().iterator().forEachRemaining(groupedResult -> {
      Map<String,String> orderedResults = new LinkedHashMap<>();
      Map<String,String> referenceResult =new LinkedHashMap<>(groupedResult.get(0));
      referenceResult.put(GbifTerm.NUM_OF_OCCURRENCES.simpleName(), referenceResult.size()+"");
      DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS.iterator().forEachRemaining( term ->
        orderedResults.put(term.simpleName(),referenceResult.get(term.simpleName()))
      );
      results.add(orderedResults);
    });
    return results;
  }
  
}
