package org.gbif.occurrence.cli.process;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.InterpretVerbatimMessage;
import org.gbif.occurrence.cli.common.HueCsvReader;

import java.io.IOException;
import java.util.List;

import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MetaInfServices(Command.class)
public class InterpretOccurrenceCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretOccurrenceCommand.class);

  private final InterpretOccurrenceConfiguration config = new InterpretOccurrenceConfiguration();

  public InterpretOccurrenceCommand() {
    super("interpret-occurrence");
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
        LOG.info("Sent interpretation message for key {}", config.key);
      } else {
        List<Integer> keys = HueCsvReader.readIntKeys(config.keyFileName);
        if (keys != null && !keys.isEmpty()) {
          int counter = 0;
          for (Integer key : keys) {
            sendDeleteMessage(publisher, key);
            counter++;
            if (counter % 10000 == 0) {
              LOG.info("Sent {} interpretation messages for key file {} so far.", counter, config.keyFileName);
            }
          }
          LOG.info("Sent {} interpretation messages for key file {}", counter, config.keyFileName);

        } else {
          LOG.warn("No keys found in {}", config.keyFileName);
        }
      }
    } catch (IOException e) {
      LOG.error("Caught exception while sending delete occurrence message", e);
    }
  }

  private void sendDeleteMessage(MessagePublisher publisher, int occurrenceKey) throws IOException {
    publisher.send(new InterpretVerbatimMessage(occurrenceKey));
  }
}
