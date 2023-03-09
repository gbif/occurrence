package org.gbif.occurrence.download.service;

public enum JobStatus {
  PREP,
  PREPPAUSED,
  PREMATER,
  PREPSUSPENDED,
  RUNNING,
  KILLED,
  RUNNINGWITHERROR,
  DONEWITHERROR,
  FAILED,
  PAUSED,
  PAUSEDWITHERROR,
  SUCCEEDED,
  SUSPENDED,
  SUSPENDEDWITHERROR,
  IGNORED
}
