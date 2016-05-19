package org.gbif.occurrence.common.interpretation;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;

import java.util.Arrays;
import java.util.List;

/**
 * InterpretationRemark is built on top of {@link OccurrenceIssue} to provide more information in the context
 * of the processing and interpretation. We want to keep this information separated from the definition of the
 * {@link OccurrenceIssue} since they could change depending of the context.
 */
public class InterpretationRemark {

  private OccurrenceIssue note;
  private InterpretationRemarkSeverity severity;
  private List<Term> relatedTerms;

  public static InterpretationRemark of(OccurrenceIssue occurrenceIssue, InterpretationRemarkSeverity severity,
                                          Term ... relatedTerms){
    return new InterpretationRemark(occurrenceIssue, severity, Arrays.asList(relatedTerms));
  }

  public InterpretationRemark(OccurrenceIssue note, InterpretationRemarkSeverity severity,
                              List<Term> relatedTerms){
    this.note = note;
    this.severity = severity;
    this.relatedTerms = relatedTerms;
  }

  public OccurrenceIssue getNote() {
    return note;
  }

  public InterpretationRemarkSeverity getSeverity() {
    return severity;
  }

  public List<Term> getRelatedTerms() {
    return relatedTerms;
  }
}
