package org.gbif.occurrence;

import java.util.Optional;

import org.gbif.api.model.occurrence.DownloadRequest;

public class KubernetesJobManager implements JobManager{

  @Override
  public Optional<String> createJob(DownloadsMessage message) {
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
