package org.gbif.occurrence.downloads.launcher.services;

import java.util.List;
import java.util.Optional;

import org.gbif.api.model.occurrence.Download;
import org.gbif.occurrence.downloads.launcher.DownloadsMessage;

public interface JobManager {

  JobStatus createJob(DownloadsMessage message);

  JobStatus cancelJob(String jobId);

  Optional<Download.Status> getStatusByName(String name);

  List<Download> renewRunningDownloadsStatuses(List<Download> downloads);

  enum JobStatus {
    RUNNING,
    FAILED,
    CANCELLED
  }
}
