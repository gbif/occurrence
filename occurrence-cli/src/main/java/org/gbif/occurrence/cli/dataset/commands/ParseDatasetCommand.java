package org.gbif.occurrence.cli.dataset.commands;

import org.gbif.cli.Command;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.ParseDatasetMessage;

import java.io.IOException;
import java.util.UUID;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MetaInfServices(Command.class)
public class ParseDatasetCommand extends DatasetMutationCommand {

  private static final Logger LOG = LoggerFactory.getLogger(ParseDatasetCommand.class);

  public ParseDatasetCommand() {
    super("parse-dataset");
  }

  @Override
  protected void sendMessage(MessagePublisher publisher, String datasetKey) throws IOException {
    publisher.send(new ParseDatasetMessage(UUID.fromString(datasetKey)));
    LOG.info("Sent message to parse occurrences for dataset [{}]", datasetKey);
  }
}
