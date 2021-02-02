package org.gbif.occurrence.cli.download;

import org.apache.avro.file.SerialAvroSplitter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.TransferDownloadToAzureMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;

/** Callback that is called when the {@link TransferDownloadToAzureMessage} is received. */
public class CloudUploaderCallback
  extends AbstractMessageCallback<TransferDownloadToAzureMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(CloudUploaderCallback.class);

  private final OccurrenceDownloadService downloadService;
  private final CloudUploaderConfiguration config;
  private final FileSystem fs;

  public CloudUploaderCallback(OccurrenceDownloadService downloadService, FileSystem fs, CloudUploaderConfiguration config) {
    this.downloadService = downloadService;
    this.config = config;
    this.fs = fs;
  }

  @Override
  public void handleMessage(TransferDownloadToAzureMessage message) {
    MDC.put("downloadKey", message.getDownloadKey());

    final String downloadKey = message.getDownloadKey();

    // Check the download exists
    Download download = downloadService.get(downloadKey);
    LOG.info("Starting upload of download file {} to Azure", downloadKey);
    if (download == null) {
      throw new RuntimeException("Download is null");
    }

    if (!download.getStatus().equals(Download.Status.SUCCEEDED)) {
      throw new RuntimeException("Download is not succeeded");
    }

    // Check the format is SIMPLE_AVRO
    if (download.getRequest().getFormat().equals(DownloadFormat.SIMPLE_AVRO)) {
      try {
        // Check the file exists in HDFS
        Path hdfsPath = new Path("/occurrence-download/dev-downloads/" + downloadKey + DownloadFormat.SIMPLE_AVRO.getExtension());
        if (!fs.exists(hdfsPath)) {
          throw new RuntimeException("Download file does not exist on HDFS");
        }
        FSDataInputStream fis = fs.open(hdfsPath);

        // Set up an UploaderToAzure with an appropriate destination
        String chunkFormat = downloadKey + "/occurrence-%05d.avro";
        LOG.debug("Uploaded chunked file will have paths like {}", chunkFormat);
        UploaderToAzure uploader =
          new UploaderToAzure(message.getEndpoint(), message.getSasToken(), message.getContainerName(), chunkFormat);

        // Feed the file through the SerialAvroSplitter, connected to the uploader
        SerialAvroSplitter avroSplitter = new SerialAvroSplitter(fis, uploader);
        avroSplitter.split();

        // Verify upload was successful etc

        // Send an email or something?

      } catch (IOException e) {

      }
    } else {
      // TODO: Just assuming a Zip file.
      try {
        // Check the file exists in HDFS
        Path hdfsPath = new Path("/occurrence-download/dev-downloads/" + downloadKey + DownloadFormat.SIMPLE_CSV.getExtension());
        if (!fs.exists(hdfsPath)) {
          throw new RuntimeException("Download file does not exist on HDFS");
        }
        FSDataInputStream fis = fs.open(hdfsPath);

        // Set up an UploaderToAzure with an appropriate destination
        String chunkFormat = downloadKey + DownloadFormat.SIMPLE_CSV.getExtension();
        LOG.debug("Uploaded file will have path {}", chunkFormat);
        UploaderToAzure uploader =
          new UploaderToAzure(message.getEndpoint(), message.getSasToken(), message.getContainerName(), chunkFormat);

        // Pass the HDFS stream to the uploader
        uploader.accept(fis, fs.getFileStatus(hdfsPath).getLen());

        // Verify upload was successful etc

        // Send an email or something?

      } catch (IOException e) {

      }


      throw new RuntimeException("Download is not SIMPLE_AVRO");
    }

    LOG.info("Uploading {} to Azure completed.", downloadKey);
  }

}
