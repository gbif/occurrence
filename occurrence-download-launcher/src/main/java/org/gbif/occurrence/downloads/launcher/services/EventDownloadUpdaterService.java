package org.gbif.occurrence.downloads.launcher.services;

import org.gbif.registry.ws.client.EventDownloadClient;
import org.springframework.stereotype.Service;

@Service
public class EventDownloadUpdaterService extends DownloadUpdaterService {

  public EventDownloadUpdaterService(EventDownloadClient eventDownloadClient) {
    super(eventDownloadClient);
  }
}
