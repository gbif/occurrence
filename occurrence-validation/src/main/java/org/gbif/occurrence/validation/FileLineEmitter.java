package org.gbif.occurrence.validation;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import akka.actor.UntypedActor;

public class FileLineEmitter extends UntypedActor {

  private OccurrenceInterpreter interpreter;

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof DataInputFile) {
      doWork((DataInputFile) message);
    } else {
      unhandled(message);
    }
  }

  private void doWork(DataInputFile dataInputFile) throws IOException {

    try (BufferedReader br = new BufferedReader(new FileReader(dataInputFile.getFileName()))) {
      String line;
      //skip the first line
      if (dataInputFile.isHasHeaders()) {
        line = br.readLine();
      }
      while ((line = br.readLine()) != null) {
        OccurrenceInterpretationResult result = interpreter.interpret(toVerbatimOccurrence(line));
        result.getUpdated().addIssue(OccurrenceIssue.BASIS_OF_RECORD_INVALID);
        getSender().tell(result);
      }
    }
  }

  private VerbatimOccurrence toVerbatimOccurrence(String line) {
    return new VerbatimOccurrence();
  }
}
