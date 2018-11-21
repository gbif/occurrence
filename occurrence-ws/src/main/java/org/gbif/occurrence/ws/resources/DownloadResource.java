/*
 * Copyright 2012 Global Biodiversity Information Facility (GBIF)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.ws.resources;

import static org.gbif.occurrence.download.service.DownloadSecurityUtil.assertLoginMatches;
import static org.gbif.occurrence.download.service.DownloadSecurityUtil.assertUserAuthenticated;

import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.bval.guice.Validate;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.SqlDownloadRequest;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.occurrence.download.service.PredicateFactory;
import org.gbif.occurrence.download.service.hive.HiveSQL;
import org.gbif.ws.util.ExtraMediaTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Path("occurrence/download/request")
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
@Consumes(MediaType.APPLICATION_JSON)
@Singleton
public class DownloadResource {

  private static final String ZIP_EXT = ".zip";
  private static final String AVRO_EXT = ".avro";

  private static final String OCCURRENCE_TABLE = "occurrence_hdfs";

  private static final Logger LOG = LoggerFactory.getLogger(DownloadResource.class);

  // low quality of source to default to JSON
  private static final String OCT_STREAM_QS = ";qs=0.5";

  private static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

  private final DownloadRequestService requestService;

  private final OccurrenceDownloadService occurrenceDownloadService;

  private final CallbackService callbackService;

  private Response describeCachedResponse;

  @Inject
  public DownloadResource(DownloadRequestService service, CallbackService callbackService,
                          OccurrenceDownloadService occurrenceDownloadService) {
    requestService = service;
    this.callbackService = callbackService;
    this.occurrenceDownloadService = occurrenceDownloadService;
  }

  @DELETE
  @Path("{key}")
  public void delDownload(@PathParam("key") String jobId, @Context SecurityContext security) {
    // service.get returns a download or throws NotFoundException
    assertLoginMatches(occurrenceDownloadService.get(jobId).getRequest(), security);
    LOG.debug("Delete download: [{}]", jobId);
    requestService.cancel(jobId);
  }

  @GET
  @Path("{key}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM + OCT_STREAM_QS)
  public InputStream getResult(@PathParam("key") String downloadKey, @Context HttpServletResponse response) {
    // if key contains avro or zip suffix remove it as we intend to work with the pure key
    downloadKey = StringUtils.removeEndIgnoreCase(downloadKey, AVRO_EXT);
    downloadKey = StringUtils.removeEndIgnoreCase(downloadKey, ZIP_EXT);

    String extension = Optional.ofNullable(occurrenceDownloadService.get(downloadKey))
                        .map(download -> (DownloadFormat.SIMPLE_AVRO == download.getRequest().getFormat())? AVRO_EXT : ZIP_EXT)
                        .orElse(ZIP_EXT);

    LOG.debug("Get download data: [{}]", downloadKey);
    // suggest filename for download in http headers
    response.setHeader("Content-Disposition", "attachment; filename=" + downloadKey + extension);
    return requestService.getResult(downloadKey);
  }

  @GET
  @Path("callback")
  public Response oozieCallback(@QueryParam("job_id") String jobId, @QueryParam("status") String status) {
    LOG.debug("Received callback from Oozie for Job [{}] with status [{}]", jobId, status);
    callbackService.processCallback(jobId, status);
    return Response.ok().build();
  }

  @GET
  @Path("sql/validate")
  @Produces(MediaType.APPLICATION_JSON)
  public Response validateSQL(@QueryParam("sql") String sqlQuery) {
    LOG.debug("Received validation request for sql query [{}]",sqlQuery);
    HiveSQL.Validate.Result result =  new HiveSQL.Validate().apply(sqlQuery);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(result).build();
  }

  @GET
  @Path("sql/describe")
  @Produces(MediaType.APPLICATION_JSON)
  public Response describeSQL() {
    LOG.debug("Received describe request for sql ");
    if (Objects.isNull(describeCachedResponse)) {
        this.describeCachedResponse = Response.ok().type(MediaType.APPLICATION_JSON)
                                      .entity(HiveSQL.Execute.describe(OCCURRENCE_TABLE)).build();
    }
    return describeCachedResponse;
  }

  @POST
  @Produces({MediaType.TEXT_PLAIN})
  @Validate
  @Path("sql")
  public String startSqlDownload(@Valid SqlDownloadRequest request, @Context SecurityContext security) {
    LOG.debug("Download: [{}]", request);
    // assert authenticated user is the same as in download
    assertLoginMatches(request, security);
    String downloadKey = requestService.create(request);
    LOG.info("Created new download job with key [{}]", downloadKey);
    return downloadKey;
  }

  /**
   * Request a new predicate download (POST method, public API).
   */
  @POST
  @Produces({MediaType.TEXT_PLAIN})
  @Validate
  public String startDownload(@Valid PredicateDownloadRequest request, @Context SecurityContext security) {
    LOG.debug("Download: [{}]", request);
    // assert authenticated user is the same as in download
    assertLoginMatches(request, security);
    String downloadKey = requestService.create(request);
    LOG.info("Created new download job with key [{}]", downloadKey);
    return downloadKey;
  }

  /**
   * Request a new download (GET method, internal API used by the portal).
   */
  @GET
  @Produces({MediaType.TEXT_PLAIN})
  public Response download(@Context HttpServletRequest httpRequest,
                         @QueryParam("notification_address") String emails,
                         @QueryParam("format") String format,
                         @Context SecurityContext securityContext) {
    String creator = assertUserAuthenticated(securityContext).getName();
    checkNotNullParameter("format", format);
    checkNotNullParameter("creator", creator);
    DownloadRequest download = downloadPredicate(httpRequest, emails, format, securityContext);
    LOG.debug("Creating download with DownloadRequest [{}] from service [{}]", download, requestService);
    try {
      String downloadKey = requestService.create(download);
      LOG.debug("Got key [{}] for new download", downloadKey);
      return Response.ok(downloadKey).build();
    } catch (WebApplicationException ex) {
      LOG.error("Error processing search-to-download request", ex);
      return ex.getResponse();
    } catch (Exception ex) {
      LOG.error("Unexpected error processing search-to-download request", ex);
      throw new WebApplicationException(Response.serverError().build());
    }
  }

  @GET
  @Path("predicate")
  public DownloadRequest downloadPredicate(@Context HttpServletRequest httpRequest,
                                           @QueryParam("notification_address") String emails,
                                           @QueryParam("format") String format,
                                           @Context SecurityContext securityContext) {
    String creator = getUserName(securityContext);
    Set<String> notificationAddress = asSet(emails);
    DownloadFormat downloadFormat = Objects.isNull(format) ? DownloadFormat.SIMPLE_CSV : DownloadFormat.valueOf(format.toUpperCase());
    if (downloadFormat.equals(DownloadFormat.SQL)) {
      String sql = httpRequest.getParameterMap().get("sql")[0];
      LOG.info("SQL build for passing to download [{}]", sql);
      return new SqlDownloadRequest(sql, creator, notificationAddress, true);
    }
    else {
      Predicate predicate = PredicateFactory.build(httpRequest.getParameterMap());
      LOG.info("Predicate build for passing to download [{}]", predicate);
      return new PredicateDownloadRequest(predicate, creator, notificationAddress, true, downloadFormat);
    }
  }

  /**
   * Gets the user name from the security context.
   */
  private static String getUserName(SecurityContext securityContext) {
    return Objects.nonNull(securityContext.getUserPrincipal()) ? securityContext.getUserPrincipal().getName() : null;
  }

  /**
   * Transforms a String that contains elements split by ',' into a Set of strings.
   */
  private static Set<String> asSet(String cvsString) {
    return Objects.nonNull(cvsString) ? Sets.newHashSet(COMMA_SPLITTER.split(cvsString)) : null;
  }

  /**
   * Validates that a parameter is not null or empty.
   */
  private static void checkNotNullParameter(String paramName, String paramValue) {
    if (Strings.isNullOrEmpty(paramValue)) {
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
        .entity("Parameter " + paramName + " can't be null").build());
    }
  }
}
