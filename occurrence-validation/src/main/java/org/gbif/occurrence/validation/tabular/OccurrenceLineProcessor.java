package org.gbif.occurrence.validation.tabular;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.validation.api.RecordProcessor;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class OccurrenceLineProcessor implements RecordProcessor<OccurrenceInterpretationResult> {

  private final OccurrenceInterpreter interpreter;
  private final Character separator;
  private final Term[] columns;

  public OccurrenceLineProcessor(OccurrenceInterpreter interpreter, Character separator, Term[] columns) {
    this.interpreter = interpreter;
    this.separator = separator;
    this.columns = columns;
  }

  @Override
  public OccurrenceInterpretationResult process(String line) {
    return interpreter.interpret(toVerbatimOccurrence(line));
  }

  /**
   * Get a {@link VerbatimOccurrence} instance from a {@Map} of {@link Term}
   *
   * @param line
   *
   * @return
   */
  private VerbatimOccurrence toVerbatimOccurrence(String line) {
    VerbatimOccurrence verbatimOccurrence = new VerbatimOccurrence();
    verbatimOccurrence.setVerbatimFields(toVerbatimMap(line));
    String datasetKey = verbatimOccurrence.getVerbatimField(GbifTerm.datasetKey);
    if (datasetKey != null) {
      verbatimOccurrence.setDatasetKey(UUID.fromString(datasetKey));
    }
    return verbatimOccurrence;
  }

  private Map<Term,String> toVerbatimMap(String line) {
    String[] values = line.split(separator.toString());
    int numOfRecords = Math.min(columns.length,values.length);
    Map<Term,String> verbatimMap = new HashMap(numOfRecords);
    for(int i = 0; i < numOfRecords; i++) {
      verbatimMap.put(columns[i],values[i]);
    }
    return verbatimMap;
  }
}
