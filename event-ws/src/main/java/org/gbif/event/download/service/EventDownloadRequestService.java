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
package org.gbif.event.download.service;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.download.service.DownloadLimitsService;
import org.gbif.occurrence.download.service.DownloadRequestServiceImpl;
import org.gbif.occurrence.mail.EmailSender;
import org.gbif.occurrence.mail.OccurrenceEmailManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EventDownloadRequestService extends DownloadRequestServiceImpl {

  @Autowired
  public EventDownloadRequestService(
      @Value("${occurrence.download.portal.url}") String portalUrl,
      @Value("${occurrence.download.ws.url}") String wsUrl,
      @Value("${occurrence.download.ws.mount}") String wsMountDir,
      OccurrenceDownloadService occurrenceDownloadService,
      DownloadLimitsService downloadLimitsService,
      OccurrenceEmailManager emailManager,
      EmailSender emailSender,
      MessagePublisher messagePublisher) {
    super(
        portalUrl,
        wsUrl,
        wsMountDir,
        occurrenceDownloadService,
        downloadLimitsService,
        emailManager,
        emailSender,
        messagePublisher,
        DownloadType.EVENT);
  }

  @Override
  protected long countRecords(Predicate predicate) {
    // it always goes thru the big downloads wf
    return -1;
  }
}
