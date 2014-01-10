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

import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.ws.security.NotAllowedException;
import org.gbif.ws.security.NotAuthenticatedException;
import org.gbif.ws.util.ExtraMediaTypes;

import java.io.InputStream;
import java.security.AccessControlException;
import java.security.Principal;

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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.bval.guice.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("occurrence/download/request")
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
@Consumes(MediaType.APPLICATION_JSON)
@Singleton
public class DownloadResource {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadResource.class);

  // low quality of source to default to JSON
  private static final String OCT_STREAM_QS = ";qs=0.5";

  private final DownloadRequestService requestService;

  private final OccurrenceDownloadService occurrenceDownloadService;

  private final CallbackService callbackService;

  @Inject
  public DownloadResource(DownloadRequestService service, CallbackService callbackService,
    OccurrenceDownloadService occurrenceDownloadService) {
    this.requestService = service;
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
    LOG.debug("Get download data: [{}]", downloadKey);
    // suggest filename for download in http headers
    response.setHeader("Content-Disposition", "attachment; filename=" + downloadKey + ".zip");
    return requestService.getResult(downloadKey);
  }


  @GET
  @Path("callback")
  public Response oozieCallback(@QueryParam("job_id") String jobId, @QueryParam("status") String status) {
    LOG.debug("Received callback from Oozie for Job [{}] with status [{}]", jobId, status);
    callbackService.processCallback(jobId, status);
    return Response.ok().build();
  }

  @POST
  @Validate
  public String startDownload(@Valid DownloadRequest request, @Context SecurityContext security) {
    LOG.debug("Download: [{}]", request);
    // assert authenticated user is the same as in download
    assertLoginMatches(request, security);
    String downloadKey = requestService.create(request);
    LOG.info("Created new download job with key [{}]", downloadKey);
    return downloadKey;
  }

  /**
   * Checks that a user is authenticated and the same user is the creator of the download.
   *
   * @throws AccessControlException if no or wrong user is authenticated
   */
  private void assertLoginMatches(DownloadRequest request, SecurityContext security) {
    // assert authenticated user is the same as in download
    final Principal principal = security.getUserPrincipal();
    if (principal == null) {
      throw new NotAuthenticatedException("No user authenticated for creating a download");
    } else if (!principal.getName().equals(request.getCreator())) {
      LOG.warn("Different user authenticated [{}] than download specifies [{}]", principal.getName(),
        request.getCreator());
      throw new NotAllowedException(principal.getName() + " not allowed to create download with creator "
        + request.getCreator());
    }
  }
}
