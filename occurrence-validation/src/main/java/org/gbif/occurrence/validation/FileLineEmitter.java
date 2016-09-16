package org.gbif.occurrence.validation;

import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import akka.actor.UntypedActor;

public class FileLineEmitter extends UntypedActor {

  private OccurrenceInterpreter interpreter;

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof String) {
      doWork((String) message);
    } else {
      unhandled(message);
    }
  }

  private void doWork(String fileName) throws IOException {

    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
      String line;
      while ((line = br.readLine()) != null) {

        getSender().tell();
      }
    }
  }
}
