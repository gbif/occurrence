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

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.pojo.AirflowConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkStaticConfiguration;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.airflow.AirflowClient;
import org.gbif.registry.ws.client.BaseDownloadClient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class AirflowDownloadLauncherServiceTest {

  private static final String DOWNLOAD_KEY = "000001";

  private BaseDownloadClient downloadClient;
  private SparkStaticConfiguration sparkStaticConfiguration;
  private AirflowDownloadLauncherService launcherService;

  @BeforeEach
  void setUp() {
    downloadClient = mock(BaseDownloadClient.class);
    sparkStaticConfiguration = new SparkStaticConfiguration();
    sparkStaticConfiguration.setMinInstances(2);
    sparkStaticConfiguration.setMaxInstances(20);
    sparkStaticConfiguration.setRecordsPerInstance(1000);
    launcherService =
        new AirflowDownloadLauncherService(
            sparkStaticConfiguration,
            new AirflowConfiguration(),
            downloadClient,
            mock(LockerService.class)) {
          @Override
          protected boolean isSmallLauncher() {
            return false;
          }

          @Override
          protected AirflowClient getAirflowClient() {
            return mock(AirflowClient.class);
          }
        };
  }

  @Test
  void markDownloadAsFailedRetriesWithFreshReadAfterConflict() throws Exception {
    Download staleDownload = new Download();
    staleDownload.setKey("stale");
    staleDownload.setStatus(Status.RUNNING);
    Download refreshedDownload = new Download();
    refreshedDownload.setKey("refreshed");
    refreshedDownload.setStatus(Status.RUNNING);

    when(downloadClient.get(DOWNLOAD_KEY)).thenReturn(staleDownload, refreshedDownload);
    doThrow(new RuntimeException("conflict"))
        .doReturn(refreshedDownload)
        .when(downloadClient)
        .update(any());

    launcherService.markDownloadAsFailed(DOWNLOAD_KEY);

    verify(downloadClient, times(2)).get(DOWNLOAD_KEY);
    verify(downloadClient, times(2)).update(any(Download.class));
    verify(downloadClient).update(staleDownload);
    verify(downloadClient).update(refreshedDownload);
  }

  @Test
  void markDownloadAsFailedStopsWhenConflictLosesToTerminalStatus() throws Exception {
    Download staleDownload = new Download();
    staleDownload.setStatus(Status.RUNNING);
    Download succeededDownload = new Download();
    succeededDownload.setStatus(Status.SUCCEEDED);

    when(downloadClient.get(DOWNLOAD_KEY)).thenReturn(staleDownload, succeededDownload);
    doThrow(new RuntimeException("conflict")).when(downloadClient).update(staleDownload);

    launcherService.markDownloadAsFailed(DOWNLOAD_KEY);

    verify(downloadClient, times(2)).get(DOWNLOAD_KEY);
    verify(downloadClient, times(1)).update(any(Download.class));
    verify(downloadClient).update(staleDownload);
    verify(downloadClient, never()).update(succeededDownload);
  }

  @Test
  void calculateExecutorInstancesFromRecordCount() {
    assertEquals(2, launcherService.calculateExecutorInstances(downloadWithRecords(0)));
    assertEquals(5, launcherService.calculateExecutorInstances(downloadWithRecords(5_000)));
    assertEquals(20, launcherService.calculateExecutorInstances(downloadWithRecords(1_000_000)));
  }

  @Test
  void calculateExecutorInstancesUsesHalfOfMaxWhenRecordCountUnknown() {
    assertEquals(10, launcherService.calculateExecutorInstances(downloadWithRecords(-1)));
  }

  @Test
  void calculateExecutorInstancesUsesConfiguredDivisorWhenRecordCountUnknown() {
    sparkStaticConfiguration.setUnknownRecordsInstancesDivisor(1.5);
    assertEquals(13, launcherService.calculateExecutorInstances(downloadWithRecords(-1)));
  }

  @Test
  void rejectsDivisorThatIsNotPositive() {
    for (double divisor : new double[] {0, -1}) {
      SparkStaticConfiguration config = new SparkStaticConfiguration();
      config.setUnknownRecordsInstancesDivisor(divisor);
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new AirflowDownloadLauncherService(
                  config, new AirflowConfiguration(), downloadClient, mock(LockerService.class)) {
                @Override
                protected boolean isSmallLauncher() {
                  return false;
                }

                @Override
                protected AirflowClient getAirflowClient() {
                  return mock(AirflowClient.class);
                }
              });
    }
  }

  @Test
  void unknownRecordsInstancesDivisorDefaultsToTwo() {
    assertEquals(2, SparkStaticConfiguration.builder().build().getUnknownRecordsInstancesDivisor());
    assertEquals(2, new SparkStaticConfiguration().getUnknownRecordsInstancesDivisor());
  }

  private static Download downloadWithRecords(long totalRecords) {
    Download download = new Download();
    download.setTotalRecords(totalRecords);
    return download;
  }
}
