package org.gbif.occurrence.download.file.specieslist;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.download.hive.DownloadTerms;

/**
 * 
 * Utility class which collects distinct species info.
 *
 */
public class SpeciesListCollector {

  private final Map<String,Map<String,String>> distinctSpeciesRecord = new HashMap<>();
  
  /**
   * @return set of records of distinct species.
   */
  public Set<Map<String, String>> getDistinctSpecies() {
    return new HashSet<>(distinctSpeciesRecord.values());
  }


  /**
   * Gets the species record by taxon key.
   */
  public Map<String,String> getByTaxonKey(String taxonKey) {
    return distinctSpeciesRecord.get(taxonKey);
  }


  /**
   * group results by taxon key and order them in {@link DownloadTerms} species list download order.
   */
  public void collect(Map<String, String> occurrenceRecord) {
    String taxonKey = coalesce(GbifTerm.acceptedTaxonKey, GbifTerm.taxonKey, occurrenceRecord);
    String scientificName = coalesce(GbifTerm.acceptedScientificName, DwcTerm.scientificName, occurrenceRecord);

    distinctSpeciesRecord.put(taxonKey, distinctSpeciesRecord.compute(taxonKey, (k, v) -> {
      //if the values are already there increment
      if (v != null) {
        long count = Long.parseLong(v.get(GbifTerm.numOfOccurrences.simpleName())) + Long.parseLong(occurrenceRecord.getOrDefault(GbifTerm.numOfOccurrences.simpleName(), "1"));
        v.put(GbifTerm.numOfOccurrences.simpleName(), Long.toString(count));
        return v;
      } else {
        occurrenceRecord.putIfAbsent(GbifTerm.numOfOccurrences.simpleName(),  Long.toString(1L));
        // order the results according to download
        return new LinkedHashMap<>(DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS.stream()
            .collect(LinkedHashMap::new, (m,val) -> {
              if (GbifTerm.taxonKey == val) {
                m.put(val.simpleName(), taxonKey);
              } else if(DwcTerm.scientificName == val) {
                m.put(val.simpleName(), scientificName);
              } else {
                m.put(val.simpleName(), occurrenceRecord.get(val.simpleName()));
              }
            }, LinkedHashMap::putAll));
      }
    }));
  }

  private static String coalesce(Term term1, Term term2, Map<String,String> record) {
    return record.getOrDefault(term1.simpleName(), record.get(term2.simpleName()));
  }
}
