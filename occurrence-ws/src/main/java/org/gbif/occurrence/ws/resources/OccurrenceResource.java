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

import com.google.common.annotations.VisibleForTesting;
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

  @VisibleForTesting
  public static final String ANNOSYS_PATH = "annosys";

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceResource.class);
  private final OccurrenceService occurrenceService;

  @Inject
  public OccurrenceResource(OccurrenceService occurrenceService) {
    this.occurrenceService = occurrenceService;
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
  public VerbatimOccurrence getVerbatim(@PathParam("key") Integer key) {
    LOG.debug("Request VerbatimOccurrence [{}]:", key);
    return occurrenceService.getVerbatim(key);
  }

  /**
   * Removed API call, which supported a stream of featured occurrences on the old GBIF.org homepage.
   * @return An empty list.
   */
  @GET
  @Path("featured")
  @Deprecated
  public List<Object> getFeaturedOccurrences() {
    LOG.warn("Featured occurrences have been removed.", e);
    return Lists.newArrayList();
  }

  /**
   * This method is implemented specifically to support Annosys and is not advertised or
   * documented in the public API.  <em>It may be removed at any time without notice</em>.
   *
   * @param key
   * @return
   */
  @GET
  @Path(ANNOSYS_PATH + "/{key}")
  @NullToNotFound
  @Produces(MediaType.APPLICATION_XML)
  public Occurrence getAnnosysOccurrence(@PathParam("key") Integer key) {
    LOG.debug("Request Annosys occurrence [{}]:", key);
    return occurrenceService.get(key);
  }

  /**
   * This method is implemented specifically to support Annosys and is not advertised or
   * documented in the public API.  <em>It may be removed at any time without notice</em>.
   *
   * @param key
   * @return
   */
  @GET
  @Path(ANNOSYS_PATH + "/{key}/" + VERBATIM_PATH)
  @NullToNotFound
  @Produces(MediaType.APPLICATION_XML)
  public VerbatimOccurrence getAnnosysVerbatim(@PathParam("key") Integer key) {
    LOG.debug("Request Annosys verbatim occurrence [{}]:", key);
    return occurrenceService.getVerbatim(key);
  }
}
