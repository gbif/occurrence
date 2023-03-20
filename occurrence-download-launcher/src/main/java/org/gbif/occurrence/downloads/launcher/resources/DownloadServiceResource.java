package org.gbif.occurrence.downloads.launcher.resources;

import org.gbif.occurrence.downloads.launcher.services.JobManager;

import org.springframework.web.bind.annotation.DeleteMapping;

import io.swagger.v3.oas.annotations.Hidden;

public class DownloadServiceResource {

  private JobManager jobManager;

  @Hidden
  @DeleteMapping("{key}")
  public void cancelJob(String jobId) {
    jobManager.cancelJob(jobId);
  }
}
