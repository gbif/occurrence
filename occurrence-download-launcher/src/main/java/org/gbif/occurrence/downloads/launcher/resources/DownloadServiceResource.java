package org.gbif.occurrence.downloads.launcher.resources;

import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.services.DownloadStatusUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.JobManager;
import org.gbif.occurrence.downloads.launcher.services.JobManager.JobStatus;
import org.gbif.occurrence.downloads.launcher.services.LockerService;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Hidden;

@RestController("launcher")
public class DownloadServiceResource {

  private final JobManager jobManager;
  private final DownloadStatusUpdaterService downloadStatusUpdaterService;
  private final LockerService lockerService;

  public DownloadServiceResource(
    @Qualifier("yarn") JobManager jobManager,
    DownloadStatusUpdaterService downloadStatusUpdaterService,
    LockerService lockerService) {
    this.jobManager = jobManager;
    this.downloadStatusUpdaterService = downloadStatusUpdaterService;
    this.lockerService = lockerService;
  }

  // TODO: ADD SECURITY: ADMIN ONLY
  @Hidden
  @DeleteMapping("{jobId}")
  public void cancelJob(@PathVariable String jobId) {
    JobStatus jobStatus = jobManager.cancelJob(jobId);
    if (jobStatus == JobStatus.CANCELLED) {
      downloadStatusUpdaterService.updateStatus(jobId, Status.CANCELLED);
    }
  }

  // TODO: ADD SECURITY: ADMIN ONLY
  @Hidden
  @DeleteMapping("/unlock")
  public void unlockAll() {
    lockerService.unlockAll();
  }
}
