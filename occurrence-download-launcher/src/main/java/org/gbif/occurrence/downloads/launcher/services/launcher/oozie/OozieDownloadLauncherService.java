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
package org.gbif.occurrence.downloads.launcher.services.launcher.oozie;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Slf4j
@Service("oozie")
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OozieDownloadLauncherService implements DownloadLauncher {

  private final OozieClient client;
  private final DownloadWorkflowParametersBuilder parametersBuilder;

  public OozieDownloadLauncherService(
      OozieClient client,
      @Qualifier("oozie.default_properties") Map<String, String> defaultProperties) {
    this.client = client;
    this.parametersBuilder = new DownloadWorkflowParametersBuilder(defaultProperties);
  }

  @Override
  public JobStatus create(@NotNull DownloadLauncherMessage message) {
    try {
      client.run(parametersBuilder.buildWorkflowParameters(message.getDownloadId(), message.getDownloadRequest()));
      return JobStatus.RUNNING;
    } catch (OozieClientException ex) {
      log.error(ex.getMessage(), ex);
      return JobStatus.FAILED;
    }
  }

  @Override
  public JobStatus cancel(@NotNull String downloadId) {
    try {
      String jobId = client.getJobId(downloadId);
      if (jobId != null && !jobId.isEmpty()) {
        client.kill(jobId);
      }
      return JobStatus.CANCELLED;
    } catch (OozieClientException ex) {
      log.error(ex.getMessage(), ex);
      return JobStatus.FAILED;
    }
  }

  @Override
  public Optional<Status> getStatusByName(String name) {
    return Optional.empty();
  }

  @Override
  public List<Download> renewRunningDownloadsStatuses(List<Download> downloads) {
    return Collections.emptyList();
  }
}
