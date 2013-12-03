package org.gbif.occurrence.cli.delete;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage;
import org.gbif.common.messaging.api.messages.OccurrenceDeletionReason;

import java.io.IOException;
import java.util.UUID;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MetaInfServices(Command.class)
public class DeleteDatasetCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteDatasetCommand.class);

  private final DeleteDatasetConfiguration config = new DeleteDatasetConfiguration();

  public DeleteDatasetCommand() {
    super("delete-dataset");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected void doRun() {
    MessagePublisher publisher;
    try {
      publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
      publisher.send(
        new DeleteDatasetOccurrencesMessage(UUID.fromString(config.uuid), OccurrenceDeletionReason.DATASET_MANUAL));
      LOG.info("Sent message to delete dataset [{}]", config.uuid);
    } catch (IOException e) {
      LOG.error("Caught exception while sending delete dataset message", e);
    }
  }
}
