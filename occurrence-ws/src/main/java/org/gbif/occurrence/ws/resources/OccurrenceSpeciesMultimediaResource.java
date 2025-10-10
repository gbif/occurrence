package org.gbif.occurrence.ws.resources;

import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.occurrence.persistence.OccurrenceSpeciesMultimediaService;
import org.springframework.beans.factory.annotation.Autowired;
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
  private OccurrenceSpeciesMultimediaService occurrenceSpeciesMultimediaService;

  /**
   * Lists for multimedia information associated with a specific species and media type, supporting pagination.
   *
   * @param speciesKey the species identifier
   * @param mediaType  the type of media (e.g., image, video)
   * @param limit      the maximum number of records to return
   * @param offset     the starting point in the collection of results
   * @return a paginated response containing multimedia information for the specified species and media type
   */
  @GetMapping(value = "species/{speciesKey}")
  public PagingResponse<OccurrenceSpeciesMultimediaService.SpeciesMediaType> listMultimediaBySpecies(@PathVariable("speciesKey") String speciesKey,
                                                                                                    @RequestParam("mediaType") String mediaType,
                                                                                                    @RequestParam("limit") int limit,
                                                                                                    @RequestParam("offset") int offset)  {
    return occurrenceSpeciesMultimediaService.queryMedianInfo(speciesKey, mediaType, Math.max(limit,0), Math.max(offset,0));
  }
}
