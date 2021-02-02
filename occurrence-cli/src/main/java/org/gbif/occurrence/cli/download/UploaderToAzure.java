package org.gbif.occurrence.cli.download;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.avro.file.SerialAvroSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;
import java.util.zip.ZipFile;

/**
 * Upload a series of files to an Azure container.
 */
public class UploaderToAzure implements Consumer<File> {
  private final BlobContainerClient containerClient;

  private static final Logger LOG = LoggerFactory.getLogger(UploaderToAzure.class);

  /* Just for development */
  public static void main(String... args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: UploaderToAzure file sasToken endpoint containerName chunkFormat");
      System.exit(1);
    }

    String filePath = args[0]; // E.g. "0000031-201028124655771/occurrence-%05d.avro"
    String sasToken = args[1]; // Looks like "?sv=2019-12-12&ss=xxxx&srt=sco&sp=xxxxxxxxx&se=2020-12-05T03:15:48Z&st=2020-11-04T19:15:48Z&spr=https,http&sig=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    String endpoint = args[2]; // Looks like "https://gbifdownloadtest2020.blob.core.windows.net/";
    String containerName = args[3]; // "dltest1";
    String chunkFormat = args[4]; // Local file path

    new SerialAvroSplitter(
      new FileInputStream(filePath),
      new UploaderToAzure(endpoint, sasToken, containerName, chunkFormat)).split();
  }

  private final String chunkFormat;
  private int fileCount = 0;

  public UploaderToAzure(String endpoint, String sasToken, String containerName, String chunkFormat) {
    /* Create a new BlobServiceClient with a SAS Token */
    BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
      .endpoint(endpoint)
      .sasToken(sasToken)
      .buildClient();

    this.containerClient = blobServiceClient.getBlobContainerClient(containerName);
    this.chunkFormat = chunkFormat;
  }

  @Override
  public void accept(File file) {
    // try {
    fileCount++;
    BlobClient blobClient = containerClient.getBlobClient(String.format(chunkFormat, fileCount));
    LOG.debug("Starting uploading chunk file {} of size {} to {}", file, file.length(), String.format(chunkFormat, fileCount));
    blobClient.uploadFromFile(file.getPath(), true); // TODO: Remove overwrite once in prod.
    LOG.debug("Completed chunk {}", file);
    // } catch (BlobStorageException ex) {
    //   if (!ex.getErrorCode().equals(BlobErrorCode.CONTAINER_ALREADY_EXISTS)) {
    //   ...
    // }
  }

  public void accept(InputStream is, long length) {
    // try {
    fileCount++;
    BlobClient blobClient = containerClient.getBlobClient(String.format(chunkFormat, fileCount));
    LOG.debug("Starting uploading InputStream of length {}", length);
    blobClient.upload(is, length, true); // TODO: Remove overwrite once in prod.
    LOG.debug("Completed uploading stream");
    // } catch (BlobStorageException ex) {
    //   if (!ex.getErrorCode().equals(BlobErrorCode.CONTAINER_ALREADY_EXISTS)) {
    //   ...
    // }
  }

  /* Idea: could also upload individual files within a zip file, even in parallel */
  public void acceptZipFile(File file) {
    try {
      ZipFile zipFile = new ZipFile(file);
      System.out.println(zipFile.stream().parallel().isParallel());

      zipFile.stream().parallel().forEach(
        zipEntry -> {
          try {
            BlobClient blobClient = containerClient.getBlobClient(zipEntry.getName() + "aoeu");
            System.out.println("Starting " + zipEntry.getName());
            // IOUtils.skipFully(zipFile.getInputStream(zipEntry), zipEntry.getSize());
            blobClient.upload(zipFile.getInputStream(zipEntry), zipEntry.getSize(), true);
            System.out.println("Completed " + zipEntry.getName());
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      );
    } catch (Exception e) {

    }
  }
}
