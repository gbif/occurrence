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
import org.gbif.registry.ws.client.EventDownloadClient;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class EventDownloadLauncherService extends AirflowDownloadLauncherService {

  private final AirflowClient eventsDownloadsAirflowClient;

  public EventDownloadLauncherService(
      SparkStaticConfiguration sparkStaticConfiguration,
      AirflowConfiguration airflowConfiguration,
      EventDownloadClient eventDownloadClient,
      LockerService lockerService) {
    super(sparkStaticConfiguration, airflowConfiguration, eventDownloadClient, lockerService);
    this.eventsDownloadsAirflowClient =
        buildAirflowClient(airflowConfiguration.eventsDownloadsAirflowDagName);
  }

  @Override
  protected AirflowClient getAirflowClient() {
    return eventsDownloadsAirflowClient;
  }

  @Override
  protected boolean isSmallLauncher() {
    return false;
  }
}
