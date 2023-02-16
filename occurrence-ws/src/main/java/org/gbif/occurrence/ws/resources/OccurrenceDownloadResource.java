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

import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.resource.DownloadResource;
import org.gbif.occurrence.download.service.CallbackService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(
  name = "Occurrence downloads",
  description = "This API provides services to request bulk downloads of occurrence records and retrieve " +
    "information about those downloads.\n\n" +
    "Occurrence downloads are created asynchronously — the user requests a download and, once complete, is sent an " +
    "email with a link to the resulting file.\n\n" +
    "It is necessary to register as a user at [GBIF.org](https://www.gbif.org/) to create a download request, " +
    "and use HTTP authentication using the username (not the email) and password.\n\n" +
    "Internally we use a [Java web service client](https://github.com/gbif/occurrence/tree/master/occurrence-ws-client) " +
    "for the consumption of these HTTP-based, RESTful web services. It may be of interest to those coding against the API.\n\n" +
    // This isn't ideal, and should be migrated to its own section or possibly non-OpenAPI documentation later.
    "### Occurrence Download Predicates\n\n" +
    "**TODO**, the explanation is probably too long for here and will refer to an external page.\n\n" +
    "For the moment, expand the `predicate` section of the request body schema on the [creation API call](#operation/requestDownload).\n" +
    "### Occurrence Download Limits\n\n" +
    "Occurrence downloads demand significant computational resources, and are monitored and limited according to the " +
    "GBIF platform load. In order to avoid that downloads requested by a single user utilize most of the resources " +
    "two rules have been set:\n\n" +
    "1. Download complexity limits:\n" +
    "   * A download predicate may contain a maximum of 101,000 items (taxon keys, kingdom keys, phylum keys, " +
    "     catalogue numbers, occurrence ids and so on).\n" +
    "   * A download predicate may contain a maximum of 10,000 points in any “within” predicate geometries.\n" +
    "2. Limits on incomplete (preparing, running) downloads:\n" +
    "   * If the total number of incomplete downloads is fewer than 100, any single user can have no more than 3 " +
    "     incomplete downloads.\n" +
    "   * If the total number of downloads is fewer than 1000 any single user may only have 1 download.\n\n" +
    "The number of user downloads currently in progress can be seen on the " +
    "[System Health page](https://www.gbif.org/system-health). Your own downloads can be seen on your " +
    "[My Downloads](https://www.gbif.org/user/download) page.",
  extensions = @io.swagger.v3.oas.annotations.extensions.Extension(
    name = "Order", properties = @ExtensionProperty(name = "Order", value = "0300"))
)
@RestController
@Validated
@RequestMapping(
    produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"},
    value = "occurrence/download/request")
@RefreshScope
public class OccurrenceDownloadResource extends DownloadResource {

  public OccurrenceDownloadResource(
      @Value("${occurrence.download.archive_server.url}") String archiveServerUrl,
      DownloadRequestService service,
      CallbackService callbackService,
      OccurrenceDownloadService occurrenceDownloadService,
      @Value("${occurrence.download.disabled:false}") Boolean downloadsDisabled) {
    super(
        archiveServerUrl,
        service,
        callbackService,
        occurrenceDownloadService,
        DownloadType.OCCURRENCE,
        downloadsDisabled);
  }
}
