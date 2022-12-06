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
package org.gbif.event.ws.resource;

import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.resource.DownloadResource;
import org.gbif.occurrence.download.service.CallbackService;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Validated
@RequestMapping(
    produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"},
    value = "event/download/request")
public class EventDownloadResource extends DownloadResource {

  public EventDownloadResource(
      @Value("${occurrence.download.archive_server.url}") String archiveServerUrl,
      DownloadRequestService service,
      CallbackService callbackService,
      OccurrenceDownloadService occurrenceDownloadService,
      @Value("${occurrence.download.disabled}") Boolean downloadsDisabled) {
    super(
        archiveServerUrl,
        service,
        callbackService,
        occurrenceDownloadService,
        DownloadType.EVENT,
        downloadsDisabled);
  }
}
