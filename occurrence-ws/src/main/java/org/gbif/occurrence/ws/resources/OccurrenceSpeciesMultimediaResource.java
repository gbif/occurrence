package org.gbif.occurrence.ws.resources;

import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.occurrence.persistence.OccurrenceSpeciesMultimediaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping(
  value = "occurrence/multimedia/",
  produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"}
)
public class OccurrenceSpeciesMultimediaResource {

  @Autowired
  private OccurrenceSpeciesMultimediaService occurrenceSpeciesMultimediaService;

  @GetMapping(value = "species/{speciesKey}")
  public PagingResponse<OccurrenceSpeciesMultimediaService.SpeciesMediaType> searchMediaBySpecies(@PathVariable("speciesKey") String speciesKey,
                                                                                                  @RequestParam("mediaType") String mediaType,
                                                                                                  @RequestParam("limit") int limit,
                                                                                                  @RequestParam("offset") int offset)  {
    return occurrenceSpeciesMultimediaService.queryIdentifiers(speciesKey, mediaType, limit, offset);
  }
}
