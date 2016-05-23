package org.gbif.occurrence.common.interpretation;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;

/**
 * InterpretationRemark is built on top of {@link OccurrenceIssue} to provide more information in the context
 * of the processing and interpretation. We want to keep this information separated from the definition of the
 * {@link OccurrenceIssue} since they could change depending of the context.
 */
public class InterpretationRemark {

  private final OccurrenceIssue type;
  private final InterpretationRemarkSeverity severity;
  private final List<Term> relatedTerms;

  public static InterpretationRemark of(OccurrenceIssue type, InterpretationRemarkSeverity severity,
                                          Term ... relatedTerms){
    Preconditions.checkNotNull(relatedTerms, "relatedTerms can not be null");
    return new InterpretationRemark(type, severity, Arrays.asList(relatedTerms));
  }

  public static InterpretationRemark of(OccurrenceIssue type, InterpretationRemarkSeverity severity,
                                        List<Term> relatedTerms){
    return new InterpretationRemark(type, severity, relatedTerms);
  }

  /**
   * Ideally, you should use one of static builders.
   *
   * @param type
   * @param severity
   * @param relatedTerms
   */
  public InterpretationRemark(OccurrenceIssue type, InterpretationRemarkSeverity severity,
                              List<Term> relatedTerms){
    this.type = type;
    this.severity = severity;
    this.relatedTerms = relatedTerms;
  }

  public OccurrenceIssue getType() {
    return type;
  }

  public InterpretationRemarkSeverity getSeverity() {
    return severity;
  }

  public List<Term> getRelatedTerms() {
    return relatedTerms;
  }
}
