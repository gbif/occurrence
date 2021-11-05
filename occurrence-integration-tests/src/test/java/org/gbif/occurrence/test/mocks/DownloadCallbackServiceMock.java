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
package org.gbif.occurrence.test.mocks;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.CallbackService;

public class DownloadCallbackServiceMock implements CallbackService {

  private final OccurrenceDownloadService occurrenceDownloadService;

  public DownloadCallbackServiceMock(OccurrenceDownloadService occurrenceDownloadService) {
    this.occurrenceDownloadService = occurrenceDownloadService;
  }

  @Override
  public void processCallback(String jobId, String status) {
    Download download = occurrenceDownloadService.get(jobId);
    download.setStatus(Download.Status.valueOf(status));
    occurrenceDownloadService.update(download);
  }
}
