package org.gbif.occurrence.downloads.launcher.resources;

import org.gbif.occurrence.downloads.launcher.services.JobManager;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Hidden;

@RestController("launcher")
public class DownloadServiceResource {

  private final JobManager jobManager;

  public DownloadServiceResource(@Qualifier("yarn") JobManager jobManager) {
    this.jobManager = jobManager;
  }

  @Hidden
  @DeleteMapping("{jobId}")
  public void cancelJob(@PathVariable String jobId) {
    jobManager.cancelJob(jobId);
  }
}
