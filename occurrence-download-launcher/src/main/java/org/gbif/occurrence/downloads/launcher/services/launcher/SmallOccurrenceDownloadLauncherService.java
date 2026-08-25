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
package org.gbif.occurrence.downloads.launcher.services.launcher;

import org.gbif.occurrence.downloads.launcher.pojo.AirflowConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkStaticConfiguration;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.airflow.AirflowClient;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/** Launcher for small occurrence downloads; always uses the small-downloads Airflow DAG. */
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SmallOccurrenceDownloadLauncherService extends AirflowDownloadLauncherService {

  private final AirflowClient airflowClient;

  public SmallOccurrenceDownloadLauncherService(
      SparkStaticConfiguration sparkStaticConfiguration,
      AirflowConfiguration airflowConfiguration,
      OccurrenceDownloadClient occurrenceDownloadClient,
      LockerService lockerService) {
    super(sparkStaticConfiguration, airflowConfiguration, occurrenceDownloadClient, lockerService);
    this.airflowClient = buildAirflowClient(airflowConfiguration.smallDownloadsAirflowDagName);
  }

  @Override
  protected AirflowClient getAirflowClient() {
    return airflowClient;
  }

  @Override
  protected boolean isSmallLauncher() {
    return true;
  }
}