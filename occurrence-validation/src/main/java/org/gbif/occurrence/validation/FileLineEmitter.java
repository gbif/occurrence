package org.gbif.occurrence.validation;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.tabular.MappedTabularDataFileReader;
import org.gbif.tabular.MappedTabularFiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import akka.actor.UntypedActor;

public class FileLineEmitter extends UntypedActor {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();
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

    //Build the columns mapping from Strings
    String[] columnsName = dataInputFile.getColumns();
    Term[] columnsMapping = new Term[columnsName.length];
    for (int i = 0; i < columnsName.length; i++) {
      columnsMapping[i] = TERM_FACTORY.findTerm(columnsName[i]);
    }

    try (MappedTabularDataFileReader<Term> mappedTabularFileReader =
                 MappedTabularFiles.newTermMappedTabularFileReader(new FileInputStream(
                                 new File(dataInputFile.getFileName())), dataInputFile.getDelimiterChar(),
                         dataInputFile.isHasHeaders(), columnsMapping)) {
      Map<Term, String> line;
      while ((line = mappedTabularFileReader.read()) != null) {
        OccurrenceInterpretationResult result = interpreter.interpret(toVerbatimOccurrence(line));
        getSender().tell(result);
      }
    }
  }

  /**
   * Get a {@link VerbatimOccurrence} instance from a {@Map} of {@link Term}
   *
   * @param line
   *
   * @return
   */
  private VerbatimOccurrence toVerbatimOccurrence(Map<Term, String> line) {
    VerbatimOccurrence verbatimOccurrence = new VerbatimOccurrence();
    verbatimOccurrence.setVerbatimFields(line);
    return verbatimOccurrence;
  }
}
