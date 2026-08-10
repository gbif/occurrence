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
package org.gbif.occurrence.ws.resources;

import org.gbif.occurrence.persistence.OccurrenceSpeciesMultimediaService;
import org.gbif.occurrence.persistence.experimental.OccurrenceSpeciesMultimediaServiceImpl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;


/**
 * RESTful resource for accessing species multimedia information from occurrence data.
 */
@RestController
@RequestMapping(
  value = "occurrence/experimental/multimedia/",
  produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"}
)
public class OccurrenceSpeciesMultimediaResource {
  @Autowired
  @Value("${defaultChecklistKey}")
  private String defaultChecklistKey;

  @Autowired
  private OccurrenceSpeciesMultimediaService occurrenceSpeciesMultimediaService;

  /**
   * Lists for multimedia information associated with a specific species and media type, supporting pagination.
   * @param checklistKey taxonomy checklist dataset key
   * @param speciesKey the species identifier
   * @param mediaType  the type of media (e.g., image, video)
   * @param limit      the maximum number of records to return
   * @param offset     the starting point in the collection of results
   * @return a paginated response containing multimedia information for the specified species and media type
   */
  @GetMapping(value = "species/{checklistKey}/{taxonKey}")
  public OccurrenceSpeciesMultimediaServiceImpl.TaxonMultimediaSearchResponse listMultimediaBySpecies(@PathVariable("checklistKey") String checklistKey,
                                                                                                      @PathVariable("taxonKey") String taxonKey,
                                                                                                      @RequestParam("mediaType") String mediaType,
                                                                                                      @RequestParam("limit") int limit,
                                                                                                      @RequestParam("offset") int offset)  {
    return occurrenceSpeciesMultimediaService.queryMediaInfo(checklistKey, taxonKey, mediaType, Math.max(limit,0), Math.max(offset,0));
  }

  /**
   * Lists for multimedia information associated with a specific species and media type, supporting pagination.
   * @param speciesKey the species identifier
   * @param mediaType  the type of media (e.g., image, video)
   * @param limit      the maximum number of records to return
   * @param offset     the starting point in the collection of results
   * @return a paginated response containing multimedia information for the specified species and media type
   */
  @GetMapping(value = "species/{taxonKey}")
  public OccurrenceSpeciesMultimediaServiceImpl.TaxonMultimediaSearchResponse listMultimediaBySpecies(@PathVariable("taxonKey") String taxonKey,
                                                                                                      @RequestParam("mediaType") String mediaType,
                                                                                                      @RequestParam("limit") int limit,
                                                                                                      @RequestParam("offset") int offset)  {
    return occurrenceSpeciesMultimediaService.queryMediaInfo(defaultChecklistKey, taxonKey, mediaType, Math.max(limit,0), Math.max(offset,0));
  }

}
