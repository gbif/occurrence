package org.gbif.occurrence.download.file.specieslist;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.gbif.dwc.terms.GbifTerm;

public enum SpeciesListCollector {
  INSTANCE;
  
  private static final String NUM_OF_OCCURRENCES = "no_of_occurrences";
  
  
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
      Map<String,String> interResult = groupedResult.get(0);
      interResult.put(NUM_OF_OCCURRENCES, String.format("{}",groupedResult.size()));
      results.add(interResult);
    });
    return results;
  }
  
}
