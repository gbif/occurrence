package org.gbif.occurrence.ws.client;

import org.gbif.ws.client.ClientFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;

public class Constants {

  public static final String OCCURRENCE_DOWNLOAD_PATH = "/occurrence/download/request";

  public static void main(String[] args) {
    ClientFactory clientFactory = new ClientFactory("https://api.gbif-dev.org/v1/");
    OccurrenceDownloadWsClient client = clientFactory.newInstance(OccurrenceDownloadWsClient.class);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    client.getStreamResult("0000693-200203081234651.zip", 10000, chunk -> {
      try {
        byteArrayOutputStream.write(IOUtils.toByteArray(chunk));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
  }

}
