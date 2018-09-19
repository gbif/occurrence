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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((distinctSpecies == null) ? 0 : distinctSpecies.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    SpeciesListResult other = (SpeciesListResult) obj;
    if (distinctSpecies == null) {
      if (other.distinctSpecies != null)
        return false;
    } else if (!distinctSpecies.equals(other.distinctSpecies))
      return false;
    return true;
  }
}
