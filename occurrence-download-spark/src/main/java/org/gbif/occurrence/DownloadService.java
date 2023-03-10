package org.gbif.occurrence;

import org.gbif.api.model.occurrence.DownloadRequest;

import java.io.Closeable;

public class DownloadService implements Closeable {

  private JobManager jobManager;

  public void cancelJob(String jobId) {
    jobManager.cancelJob(jobId);
  }

  public String createJob(String jobId, DownloadRequest downloadRequest) {
    return jobManager.createJob(jobId, downloadRequest);
  }

  @Override
  public void close() {
    jobManager.close();
  }
}
