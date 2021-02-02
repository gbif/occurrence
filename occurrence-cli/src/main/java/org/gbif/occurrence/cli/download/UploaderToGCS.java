package org.gbif.occurrence.cli.download;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.avro.file.SerialAvroSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Consumer;

/**
 * Draft code, probably doesn't work.
 */
public class UploaderToGCS implements Consumer<File> {
  private static final Logger LOG = LoggerFactory.getLogger(UploaderToGCS.class);

  private final String bucketName;
  private final Storage storage;

  public static void main(String... args) throws Exception {

    if (args.length != 4) {
      System.err.println("Usage: UploaderToAzure sasToken endpoint containerName file");
      System.exit(1);
    }

    String sasToken = args[0];
    String endpoint = args[1];
    String containerName = args[2];
    String filePath = args[3];

    new SerialAvroSplitter(
      new FileInputStream(filePath),
      new UploaderToGCS(endpoint, sasToken, containerName, "0000031-201028124655771/occurrence-%05d.avro")).split();
  }

  private final String chunkFormat;
  private int fileCount = 0;

  public UploaderToGCS(String projectId, String sasToken, String bucketName, String chunkFormat) {
    // Instantiates a client
    storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

    // Get bucket
    Bucket bucket = storage.get(bucketName);
    this.bucketName = bucketName;

    this.chunkFormat = chunkFormat;
  }

  @Override
  public void accept(File file) {
    try {
      fileCount++;
      BlobId blobId = BlobId.of(bucketName, String.format(chunkFormat, fileCount));
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
      LOG.debug("Starting uploading chunk file {} of size {} to {}", file, file.length(), String.format(chunkFormat, fileCount));
      storage.create(blobInfo, Files.readAllBytes(file.toPath()));
      LOG.debug("Completed chunk {}", file);
    } catch (IOException e) {

    }
  }
}
