package org.gbif.occurrence.downloads.launcher.services;

import java.util.Optional;

import org.gbif.occurrence.downloads.launcher.DownloadsMessage;

public interface JobManager {

  Optional<String> createJob(DownloadsMessage message);

  void cancelJob(String jobId);

}
