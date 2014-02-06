package org.gbif.occurrence.cli.dataset.commands;

import org.gbif.cli.Command;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage;
import org.gbif.common.messaging.api.messages.OccurrenceDeletionReason;

import java.io.IOException;
import java.util.UUID;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MetaInfServices(Command.class)
public class DeleteDatasetCommand extends DatasetMutationCommand {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteDatasetCommand.class);

  public DeleteDatasetCommand() {
    super("delete-dataset");
  }

  @Override
  protected void sendMessage(MessagePublisher publisher, String datasetKey) throws IOException {
    publisher
      .send(new DeleteDatasetOccurrencesMessage(UUID.fromString(datasetKey), OccurrenceDeletionReason.DATASET_MANUAL));
    LOG.info("Sent message to delete occurrences for dataset [{}]", datasetKey);
  }
}
