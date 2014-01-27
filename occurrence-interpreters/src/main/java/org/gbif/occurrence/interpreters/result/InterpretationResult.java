package org.gbif.occurrence.interpreters.result;

import org.gbif.api.vocabulary.OccurrenceValidationRule;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * Base class for interpretation results that modify validation rules.
 */
public class InterpretationResult {
  private final Map<OccurrenceValidationRule, Boolean> rules = Maps.newHashMap();

  public void setValidationRule(OccurrenceValidationRule rule, boolean result) {
    rules.put(rule, result);
  }

  public Map<OccurrenceValidationRule, Boolean> getRules() {
    return rules;
  }
}
