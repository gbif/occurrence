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
import java.io.InputStream;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import org.apache.bval.guice.Validate;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.SqlDownloadRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.occurrence.ws.provider.hive.HiveSQL;
import org.gbif.ws.util.ExtraMediaTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Path("occurrence/download/request")
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
@Consumes(MediaType.APPLICATION_JSON)
@Singleton
public class DownloadResource {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadResource.class);

  // low quality of source to default to JSON
  private static final String OCT_STREAM_QS = ";qs=0.5";
  private static final ObjectMapper mapper = new ObjectMapper();
  private final DownloadRequestService requestService;

  private final OccurrenceDownloadService occurrenceDownloadService;

  private final CallbackService callbackService;
  
  private String describeResponseCache = "";

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
    downloadKey = StringUtils.removeEndIgnoreCase(downloadKey, ".avro");
    downloadKey = StringUtils.removeEndIgnoreCase(downloadKey, ".zip");

    String extension = ".zip";
    Download download = occurrenceDownloadService.get(downloadKey);
    if (download != null) {
      extension = (download.getRequest().getFormat() == DownloadFormat.SIMPLE_AVRO) ? ".avro" : ".zip";
    }

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
  public Response validateSQL(@QueryParam("sql") String sqlquery) {
    LOG.debug("Received validation request for sql query [{}]",sqlquery);
    HiveSQL.Validate.Result result =  new HiveSQL.Validate().apply(sqlquery);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(result.toString()).build();
  }
  
  @GET
  @Path("sql/describe")
  @Produces(MediaType.APPLICATION_JSON)
  public Response describeSQL() {
    LOG.debug("Received describe request for sql ");
    if (describeResponseCache.isEmpty()) {
      try {
        describeResponseCache = mapper.writeValueAsString(HiveSQL.Execute.describe("occurrence_hdfs"));
      } catch (Exception ex) {
        Throwables.propagate(ex);
      }
    }
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(describeResponseCache).build();
  }
  
  @POST
  @Validate
  @Path("sql")
  public String startDownload(@Valid SqlDownloadRequest request, @Context SecurityContext security) {
    LOG.debug("Download: [{}]", request);

    // assert authenticated user is the same as in download
    assertLoginMatches(request, security);
    String downloadKey = requestService.create(request);
    LOG.info("Created new download job with key [{}]", downloadKey);
    return downloadKey;
  }
  
  @POST
  @Validate
  public String startDownload(@Valid PredicateDownloadRequest request, @Context SecurityContext security) {
    LOG.debug("Download: [{}]", request);
    // assert authenticated user is the same as in download
    assertLoginMatches(request, security);
    String downloadKey = requestService.create(request);
    LOG.info("Created new download job with key [{}]", downloadKey);
    return downloadKey;
  }

}
