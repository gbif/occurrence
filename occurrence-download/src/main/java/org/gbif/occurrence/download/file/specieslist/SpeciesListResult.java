package org.gbif.occurrence.download.file.specieslist;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.download.file.Result;

/**
 * 
 * Customizing {@link org.gbif.occurrence.download.file.Result} to add distinct species list.
 *
 */
public class SpeciesListResult extends Result {

  private final Set<Map<String, String>> distinctSpecies;

  public SpeciesListResult(DownloadFileWork work, Map<UUID, Long> datasetUsages, Set<License> datasetLicenses,
      Set<Map<String, String>> distinctSpecies) {
    super(work, datasetUsages, datasetLicenses);
    this.distinctSpecies = distinctSpecies;
  }

  public Set<Map<String, String>> getDistinctSpecies() {
    return distinctSpecies;
  }
}
