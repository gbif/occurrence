package org.gbif.occurrence;

import org.gbif.api.model.occurrence.DownloadRequest;

public class KubernetesJobManager implements JobManager{

  @Override
  public String createJob(String jobId, DownloadRequest downloadRequest) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public void cancelJob(String jobId) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("The method is not implemented!");
  }
}
