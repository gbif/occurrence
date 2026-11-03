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
package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.mail.EmailSender;
import org.gbif.occurrence.mail.OccurrenceEmailManager;
import org.gbif.occurrence.query.PredicateCounter;
import org.gbif.occurrence.query.PredicateGeometryPointCounter;
import org.gbif.registry.ws.client.DoiInteractionClient;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class OccurrenceDownloadRequestService extends DownloadRequestServiceImpl {

  private final OccurrenceSearchService occurrenceSearchService;

  @Value("${occurrence.download.small_download_cut_off}")
  private int smallDownloadCutOff;

  // queries with more predicate values than this don't use Elasticsearch, a negative value disables
  // the check
  @Value("${occurrence.download.small_download_max_predicate_values:1000}")
  private int smallDownloadMaxPredicateValues = 1000;

  private final PredicateCounter predicateCounter = new PredicateCounter();
  private final PredicateGeometryPointCounter predicateGeometryPointCounter =
      new PredicateGeometryPointCounter();

  @Autowired
  public OccurrenceDownloadRequestService(
      @Value("${occurrence.download.portal.url}") String portalUrl,
      @Value("${occurrence.download.ws.url}") String wsUrl,
      @Value("${occurrence.download.ws.mount}") String wsMountDir,
      OccurrenceDownloadService occurrenceDownloadService,
      OccurrenceSearchService occurrenceSearchService,
      DownloadLimitsService downloadLimitsService,
      OccurrenceEmailManager emailManager,
      EmailSender emailSender,
      MessagePublisher messagePublisher,
      DoiInteractionClient doiInteractionClient,
      @Value("${checklist.nested.struct.config: {}}") String checklistNestedStructConfigJson) {
    super(
        portalUrl,
        wsUrl,
        wsMountDir,
        occurrenceDownloadService,
        downloadLimitsService,
        emailManager,
        emailSender,
        messagePublisher,
        DownloadType.OCCURRENCE,
        doiInteractionClient,
        checklistNestedStructConfigJson
    );
    this.occurrenceSearchService = occurrenceSearchService;
  }

  @Override
  protected int getSmallDownloadCutOff() {
    return smallDownloadCutOff;
  }

  @Override
  protected long countRecords(Predicate predicate) {
    if (smallDownloadMaxPredicateValues >= 0) {
      // predicate values (each IN value counts separately) plus geometry points
      int values = predicateCounter.count(predicate) + predicateGeometryPointCounter.count(predicate);
      if (values > smallDownloadMaxPredicateValues) {
        // skip the Elasticsearch count, -1 is never a small download so this goes to Spark
        log.info(
            "Query has {} predicate values, more than {}, not counting with Elasticsearch",
            values, smallDownloadMaxPredicateValues);
        return -1;
      }
    }
    return occurrenceSearchService.countRecords(predicate);
  }
}
