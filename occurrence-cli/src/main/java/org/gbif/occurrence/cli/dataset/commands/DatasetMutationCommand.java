package org.gbif.occurrence.cli.dataset.commands;

import org.gbif.cli.BaseCommand;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.cli.common.HueCsvReader;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DatasetMutationCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetMutationCommand.class);

  protected final DatasetMutationConfiguration config = new DatasetMutationConfiguration();

  protected DatasetMutationCommand(String name) {
    super(name);
  }

  @Override
  protected void doRun() {
    if (config.uuid == null && config.uuidFileName == null) {
      LOG.error("Both uuid and uuidFileName are null - one of them has to be set. Exiting.");
      return;
    }

    try {
      MessagePublisher publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
      if (config.uuid != null) {
        sendMessage(publisher, config.uuid);
      } else {
        List<String> keys = HueCsvReader.readKeys(config.uuidFileName);
        if (keys != null && !keys.isEmpty()) {
          for (String key : keys) {
            sendMessage(publisher, key);
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Caught exception while sending dataset mutation message", e);
    }
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  protected abstract void sendMessage(MessagePublisher publisher, String datasetKey) throws IOException;
}
