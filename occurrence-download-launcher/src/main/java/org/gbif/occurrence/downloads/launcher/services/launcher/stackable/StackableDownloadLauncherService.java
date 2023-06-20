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
package org.gbif.occurrence.downloads.launcher.services.launcher.stackable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.pojo.DistributedConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.MainSparkSettings;
import org.gbif.occurrence.downloads.launcher.pojo.SparkConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.StackableConfiguration;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StackableDownloadLauncherService implements DownloadLauncher {
  private final DistributedConfiguration distributedConfiguration;
  private final SparkConfiguration sparkConfiguration;
  private final StackableConfiguration stackableConfiguration;
  private final LockerService lockerService;

  public StackableDownloadLauncherService(
      DistributedConfiguration distributedConfiguration,
      SparkConfiguration sparkConfiguration,
      StackableConfiguration stackableConfiguration,
      LockerService lockerService) {
    this.distributedConfiguration = distributedConfiguration;
    this.sparkConfiguration = sparkConfiguration;
    this.stackableConfiguration = stackableConfiguration;
    this.lockerService = lockerService;
  }

  @Override
  public JobStatus create(DownloadLauncherMessage message) {

    try {
      MainSparkSettings sparkSettings =
          MainSparkSettings.builder().executorMemory("4").parallelism(4).executorNumbers(2).build();

      StackableSparkRunner.builder()
          .distributedConfig(distributedConfiguration)
          .sparkConfig(sparkConfiguration)
          .kubeConfigFile(stackableConfiguration.kubeConfigFile)
          .sparkCrdConfigFile(stackableConfiguration.sparkCrdConfigFile)
          .sparkAppName(message.getDownloadKey())
          .deleteOnFinish(stackableConfiguration.deletePodsOnFinish)
          .sparkSettings(sparkSettings)
          .lockerService(lockerService)
          .build()
          .start()
          .asyncStatusCheck();

      return JobStatus.RUNNING;
    } catch (Exception ex) {
      return JobStatus.FAILED;
    }
  }

  @Override
  public JobStatus cancel(String downloadKey) {
    //    try {
    //      sparkController.stopSparkApplication(downloadKey);
    //      return JobStatus.CANCELLED;
    //    } catch (ApiException e) {
    return JobStatus.FAILED;
    //    }
  }

  @Override
  public Optional<Status> getStatusByName(String downloadKey) {
    //    K8StackableSparkController.Phase phase =
    //        k8StackableSparkController.getApplicationPhase(downloadKey);
    //    if (phase == Phase.RUNNING) {
    //      return Optional.of(Status.RUNNING);
    //    }
    //    if (phase == Phase.SUCCEEDED) {
    //      return Optional.of(Status.SUCCEEDED);
    //    }
    return Optional.empty();
  }

  @Override
  public List<Download> renewRunningDownloadsStatuses(List<Download> downloads) {
    return Collections.emptyList();
    // throw new UnsupportedOperationException("The method is not implemented!");
  }
}
