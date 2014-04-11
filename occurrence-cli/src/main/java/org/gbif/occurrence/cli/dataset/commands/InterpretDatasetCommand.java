package org.gbif.occurrence.cli.dataset.commands;

import org.gbif.cli.Command;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.InterpretDatasetMessage;

import java.io.IOException;
import java.util.UUID;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MetaInfServices(Command.class)
public class InterpretDatasetCommand extends DatasetMutationCommand {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretDatasetCommand.class);

  public InterpretDatasetCommand() {
    super("interpretMedia-dataset");
  }

  @Override
  protected void sendMessage(MessagePublisher publisher, String datasetKey) throws IOException {
    publisher.send(new InterpretDatasetMessage(UUID.fromString(datasetKey)));
    LOG.info("Sent message to interpretMedia occurrences for dataset [{}]", datasetKey);
  }
}
