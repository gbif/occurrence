package org.gbif.occurrence.downloads.launcher.services;

import java.util.Optional;

import org.gbif.occurrence.downloads.launcher.DownloadsMessage;

import org.springframework.stereotype.Service;

@Service("kubernetes")
public class KubernetesJobManagerService implements JobManager {

  @Override
  public Optional<String> createJob(DownloadsMessage message) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public void cancelJob(String jobId) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }
}
