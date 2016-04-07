package org.gbif.occurrence.cli.delete;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DeleteOccurrenceMessage;
import org.gbif.common.messaging.api.messages.OccurrenceDeletionReason;
import org.gbif.occurrence.cli.common.HueCsvReader;

import java.io.IOException;
import java.util.List;

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
    if (config.key == null && config.keyFileName == null) {
      LOG.error("Both key and keyFileName are null - one of them has to be set. Exiting.");
      return;
    }

    try {
      MessagePublisher publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
      if (config.key != null) {
        sendDeleteMessage(publisher, config.key);
      } else {
        List<String> keys = HueCsvReader.readKeys(config.keyFileName);
        if (keys != null && !keys.isEmpty()) {
          for (String key : keys) {
            sendDeleteMessage(publisher, Integer.valueOf(key));
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Caught exception while sending delete occurrence message", e);
    }
  }

  private static void sendDeleteMessage(MessagePublisher publisher, int occurrenceKey) throws IOException {
    publisher.send(new DeleteOccurrenceMessage(occurrenceKey, OccurrenceDeletionReason.OCCURRENCE_MANUAL, null, null));
    LOG.info("Sent message to delete occurrence [{}]", occurrenceKey);
  }
}
