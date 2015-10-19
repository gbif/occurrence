package org.gbif.occurrence.ws.resources;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.ws.server.interceptor.NullToNotFound;
import org.gbif.ws.util.ExtraMediaTypes;

import java.io.IOException;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.ws.paths.OccurrencePaths.FRAGMENT_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.VERBATIM_PATH;

/**
 * Occurrence resource, and the verbatim sub resource.
 */
@Path(OCCURRENCE_PATH)
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
public class OccurrenceResource {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceResource.class);
  private final OccurrenceService occurrenceService;
  private final FeaturedOccurrenceReader featuredOccurrenceReader;

  @Inject
  public OccurrenceResource(OccurrenceService occurrenceService, FeaturedOccurrenceReader featuredOccurrenceReader) {
    this.occurrenceService = occurrenceService;
    this.featuredOccurrenceReader = featuredOccurrenceReader;
  }

  /**
   * This retrieves a single Occurrence detail by its key from HBase.
   *
   * @param key Occurrence key
   * @return requested Occurrence or null if none could be found
   */
  @GET
  @Path("/{id}")
  @NullToNotFound
  @Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT, MediaType.APPLICATION_XML})
  public Occurrence get(@PathParam("id") Integer key) {
    LOG.debug("Request Occurrence [{}]:", key);
    return occurrenceService.get(key);
  }

  /**
   * This retrieves a single occurrence fragment in its raw form as a string.
   *
   * @param key The Occurrence key
   * @return requested occurrence fragment or null if none could be found
   */
  @GET
  @Path("/{key}/" + FRAGMENT_PATH)
  @NullToNotFound
  public String getFragment(@PathParam("key") Integer key) {
    LOG.debug("Request occurrence fragment [{}]:", key);
    return occurrenceService.getFragment(key);
  }

  /**
   * This retrieves a single VerbatimOccurrence detail by its key from HBase and transforms it into the API
   * version which uses Maps.
   *
   * @param key The Occurrence key
   * @return requested VerbatimOccurrence or null if none could be found
   */
  @GET
  @Path("/{key}/" + VERBATIM_PATH)
  @NullToNotFound
  @Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT, MediaType.APPLICATION_XML})
  public VerbatimOccurrence getVerbatim(@PathParam("key") Integer key) {
    LOG.debug("Request VerbatimOccurrence [{}]:", key);
    return occurrenceService.getVerbatim(key);
  }

  /**
   * This is an HTTP only api call, to supply the stream of featured occurrences for the dots on the map.
   * @return The page of featured occurrences which is randomized.
   */
  @GET
  @Path("featured")
  public List<FeaturedOccurrence> getFeaturedOccurrences() {
    try {
      return featuredOccurrenceReader.featuredOccurrences();
    } catch (IOException e) {
      LOG.error("Unable to read featured occurrences", e);
      return Lists.newArrayList();
    }
  }
}
