package org.gbif.occurrence.downloads.launcher.services;

import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

/**
 * Service to be called from a SparkOutputListener to update a status of a download or called by CRON to get list of
 * unfinished downloads and check their status
 */
@Slf4j
@Service
public class DownloadStatusUpdaterService {

  public void updateStatus(String jodId, String status){
    log.info("Update status for jobId {}, status {}", jodId, status);
  }

}
