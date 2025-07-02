package org.gbif.occurrence.downloads.launcher.services;


import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.springframework.stereotype.Service;

@Service
public class OccurrenceDownloadUpdaterService extends DownloadUpdaterService {

  public OccurrenceDownloadUpdaterService(OccurrenceDownloadClient occurrenceDownloadClient) {
    super(occurrenceDownloadClient);
  }
}
