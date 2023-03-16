package org.gbif.occurrence;

import org.gbif.api.model.occurrence.DownloadRequest;

import java.io.Closeable;
import java.util.Optional;

public class DownloadServiceResource implements DownloadService {

  private JobManager jobManager;

  @Override
  public void cancelJob(String jobId) {
    jobManager.cancelJob(jobId);
  }

  @Override
  public String createJob(DownloadsMessage message) {
    return jobManager.createJob(message).orElse("Error");
  }
}
