package org.gbif.occurrence.downloads.launcher.services;

import java.util.Optional;

import org.gbif.occurrence.downloads.launcher.DownloadsMessage;

public class KubernetesJobManagerService implements JobManager{

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
