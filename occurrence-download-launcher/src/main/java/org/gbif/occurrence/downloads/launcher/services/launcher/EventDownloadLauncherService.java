package org.gbif.occurrence.downloads.launcher.services.launcher;

import org.gbif.api.model.occurrence.Download;
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
  protected AirflowClient getAirflowClient(Download download) {
    return eventsDownloadsAirflowClient;
  }
}
