package org.gbif.occurrence;

import java.util.Optional;

import org.gbif.api.model.occurrence.DownloadRequest;

public interface JobManager {

  Optional<String> createJob(String jobId, DownloadRequest downloadRequest);

  void cancelJob(String jobId);

  void close();

}
