package org.gbif.occurrence.downloads.launcher.services.launcher;

import org.gbif.api.model.occurrence.Download;
import org.gbif.occurrence.downloads.launcher.pojo.AirflowConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkStaticConfiguration;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.airflow.AirflowClient;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OccurrenceDownloadLauncherService extends AirflowDownloadLauncherService {

  private final AirflowClient bigDownloadsAirflowClient;
  private final AirflowClient smallDownloadsAirflowClient;

  public OccurrenceDownloadLauncherService(
      SparkStaticConfiguration sparkStaticConfiguration,
      AirflowConfiguration airflowConfiguration,
      OccurrenceDownloadClient occurrenceDownloadClient,
      LockerService lockerService) {
    super(sparkStaticConfiguration, airflowConfiguration, occurrenceDownloadClient, lockerService);
    this.bigDownloadsAirflowClient =
        buildAirflowClient(airflowConfiguration.bigDownloadsAirflowDagName);
    this.smallDownloadsAirflowClient =
        buildAirflowClient(airflowConfiguration.smallDownloadsAirflowDagName);
  }

  @Override
  protected AirflowClient getAirflowClient(Download download) {
    return isSmallDownload(download) ? smallDownloadsAirflowClient : bigDownloadsAirflowClient;
  }
}
