package org.gbif.occurrence.downloads.launcher.services.launcher;

import org.gbif.occurrence.downloads.launcher.pojo.AirflowConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkStaticConfiguration;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.airflow.AirflowClient;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/** Launcher for large occurrence downloads; always uses the big-downloads Airflow DAG. */
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OccurrenceDownloadLauncherService extends AirflowDownloadLauncherService {

  private final AirflowClient airflowClient;

  public OccurrenceDownloadLauncherService(
      SparkStaticConfiguration sparkStaticConfiguration,
      AirflowConfiguration airflowConfiguration,
      OccurrenceDownloadClient occurrenceDownloadClient,
      LockerService lockerService) {
    super(sparkStaticConfiguration, airflowConfiguration, occurrenceDownloadClient, lockerService);
    this.airflowClient = buildAirflowClient(airflowConfiguration.bigDownloadsAirflowDagName);
  }

  @Override
  protected AirflowClient getAirflowClient() {
    return airflowClient;
  }

  @Override
  protected boolean isSmallLauncher() {
    return false;
  }
}