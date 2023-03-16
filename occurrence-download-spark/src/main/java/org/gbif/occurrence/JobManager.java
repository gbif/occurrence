package org.gbif.occurrence;

import java.util.Optional;

import org.gbif.api.model.occurrence.DownloadRequest;

public interface JobManager {

  Optional<String> createJob(DownloadsMessage message);

  void cancelJob(String jobId);

  void close();

}
