package org.gbif.occurrence.download.file.specieslist;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.download.hive.DownloadTerms;
import com.google.common.io.Files;

public class SpeciesListCollector {

  private ObjectMapper mapper = new ObjectMapper();
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
  public void computeDistinctSpecies(List<Map<String, String>> filteredResult) {
    if (!distinctSpeciesRecord.isEmpty())
      distinctSpeciesRecord.clear();
    Map<String, List<Map<String, String>>> groupByTaxonKey = filteredResult.stream()
        .collect(Collectors.groupingBy(occMap -> occMap.get(GbifTerm.taxonKey.simpleName())));
    
    groupByTaxonKey.values().iterator().forEachRemaining(groupedResult -> {
      Map<String, String> orderedResults = new LinkedHashMap<>();
      // takes reference values for the download results
      Map<String, String> referenceResult = new LinkedHashMap<>(groupedResult.get(0));
      referenceResult.put(GbifTerm.numOfOccurrences.simpleName(), Long.toString(groupedResult.size()));
      // order the map according to download terms declaration
      DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS.iterator().forEachRemaining(
          term -> orderedResults.put(term.simpleName(), referenceResult.get(term.simpleName())));
      distinctSpeciesRecord.add(orderedResults);
    });
  }
  
  /**
   * Serializes the species list to String.
   * @return species list in json.
   * @throws IOException
   */
  public void persist(File persistFile) throws IOException {
    Files.write(mapper.writer().writeValueAsString(this.distinctSpeciesRecord), persistFile, StandardCharsets.UTF_8);
  }
  
  /**
   * Parse unique species list from the json.
   * @param jsonString
   * @return
   * @throws IOException
   */
  public static List<Map<String, String>> read(File dataFile) throws IOException {
    String serializedJson = Files.toString(dataFile, StandardCharsets.UTF_8);
    TypeReference<List<Map<String, String>>> typeRef = new TypeReference<List<Map<String, String>>>() {};
    return new ObjectMapper().readValue(serializedJson, typeRef);
  }
  
}
