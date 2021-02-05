package org.gbif.occurrence.cli.download.transfer;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.TransferDownloadToAzureMessage;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Starts an upload by sending a message (which then needs to be picked up by the Uploader).
 *
 * Example:
 *   java -jar occurrence-cli.jar upload-to-azure --conf ~crap/config/occurrence-cloud-transfer-azure.yaml \
 *     --download-key 0000075-201112155919426 \
 *     --sas-token '?sv=2019-12-12&ss=XXXX&srt=sco&sp=XXXXXXXXX&se=2020-12-05T03:15:48Z&st=2020-11-04T19:15:48Z&spr=https,http&sig=XXXXXXXX' \
 *     --endpoint 'https://gbifdownloadtest2020.blob.core.windows.net/' \
 *     --container-name dltest1
 */
@MetaInfServices(Command.class)
public class UploadToAzure extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(UploadToAzure.class);

  private final UploadToAzureConfiguration config = new UploadToAzureConfiguration();

  public UploadToAzure() {
    super("upload-to-azure");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected void doRun() {
    try {
      MessagePublisher publisher =
          new DefaultMessagePublisher(config.messaging.getConnectionParameters());
      Message message = new TransferDownloadToAzureMessage(config.downloadKey, config.sasToken, config.endpoint, config.containerName);
      publisher.send(message);
      LOG.info("Sent message to upload {} to Azure", config.downloadKey);
    } catch (IOException e) {
      LOG.error("Caught exception while sending upload", e);
    }
  }
}
