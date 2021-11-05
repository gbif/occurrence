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

import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.download.file.Result;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
