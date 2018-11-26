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
import java.security.Principal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import com.google.common.base.Preconditions;
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
import org.gbif.api.util.VocabularyUtils;
import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.occurrence.download.service.PredicateFactory;
import org.gbif.occurrence.download.service.hive.HiveSQL;
import org.gbif.occurrence.download.service.hive.Result;
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

  private List<Result.DescribeResult> describeCachedResponse;

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
  public HiveSQL.Validate.Result validateSQL(@QueryParam("sql") String sqlQuery) {
    LOG.debug("Received validation request for sql query [{}]", sqlQuery);
    return new HiveSQL.Validate().apply(sqlQuery);
  }

  @GET
  @Path("sql/describe")
  @Produces(MediaType.APPLICATION_JSON)
  public List<Result.DescribeResult> describeSQL() {
    LOG.debug("Received describe request for sql ");
    if (Objects.isNull(describeCachedResponse)) {
      this.describeCachedResponse = HiveSQL.Execute.describe(OCCURRENCE_TABLE);
    }
    return describeCachedResponse;
  }

  @POST
  @Produces({MediaType.TEXT_PLAIN})
  @Validate
  @Path("sql")
  public String startSqlDownload(@NotNull @Valid SqlDownloadRequest request, @Context SecurityContext security) {
    return createDownload(request, security);
  }

  /**
   * Request a new predicate download (POST method, public API).
   */
  @POST
  @Produces({MediaType.TEXT_PLAIN})
  @Validate
  public String startDownload(@NotNull @Valid PredicateDownloadRequest request, @Context SecurityContext security) {
    return createDownload(request, security);
  }

  /**
   * Creates/Starts an occurrence download.
   */
  private String createDownload(DownloadRequest downloadRequest, @Context SecurityContext securityContext) {
    Principal userAuthenticated = assertUserAuthenticated(securityContext);
    if (Objects.isNull(downloadRequest.getCreator())) {
      downloadRequest.setCreator(userAuthenticated.getName());
    }
    LOG.debug("Download: [{}]", downloadRequest);
    // assert authenticated user is the same as in download
    assertLoginMatches(downloadRequest, userAuthenticated);
    String downloadKey = requestService.create(downloadRequest);
    LOG.info("Created new download job with key [{}]", downloadKey);
    return downloadKey;
  }

  /**
   * Request a new download (GET method, internal API used by the portal).
   */
  @GET
  @Produces({MediaType.TEXT_PLAIN})
  public String download(@Context HttpServletRequest httpRequest, @QueryParam("notification_address") String emails,
                         @QueryParam("format") String format, @Context SecurityContext securityContext) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(format), "Format can't be null");
    return createDownload(downloadPredicate(httpRequest, emails, format, securityContext), securityContext);
  }

  @GET
  @Path("predicate")
  public DownloadRequest downloadPredicate(@Context HttpServletRequest httpRequest,
                                           @QueryParam("notification_address") String emails,
                                           @QueryParam("format") String format,
                                           @Context SecurityContext securityContext) {
    DownloadFormat downloadFormat = VocabularyUtils.lookupEnum(format,DownloadFormat.class);
    Preconditions.checkArgument(Objects.nonNull(downloadFormat), "Format param is not present");
    String creator = getUserName(securityContext);
    Set<String> notificationAddress = asSet(emails);
    if (DownloadFormat.SQL == downloadFormat) {
      String sql = httpRequest.getParameter("sql");
      LOG.info("SQL build for passing to download [{}]", sql);
      return new SqlDownloadRequest(sql, creator, notificationAddress, true);
    } else {
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
}
