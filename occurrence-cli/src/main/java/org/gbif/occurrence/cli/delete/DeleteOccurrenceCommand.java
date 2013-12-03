package org.gbif.occurrence.cli.delete;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DeleteOccurrenceMessage;
import org.gbif.common.messaging.api.messages.OccurrenceDeletionReason;

import java.io.IOException;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MetaInfServices(Command.class)
public class DeleteOccurrenceCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteOccurrenceCommand.class);

  private final DeleteOccurrenceConfiguration config = new DeleteOccurrenceConfiguration();

  public DeleteOccurrenceCommand() {
    super("delete-occurrence");
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
      publisher.send(new DeleteOccurrenceMessage(config.key, OccurrenceDeletionReason.OCCURRENCE_MANUAL, null, null));
      LOG.info("Sent message to delete occurrence [{}]", config.key);
    } catch (IOException e) {
      LOG.error("Caught exception while sending delete occurrence message", e);
    }
  }
}
