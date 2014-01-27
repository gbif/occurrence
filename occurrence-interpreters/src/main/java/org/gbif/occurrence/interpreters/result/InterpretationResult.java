package org.gbif.occurrence.interpreters.result;

import org.gbif.api.vocabulary.OccurrenceValidationRule;

import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Base class for interpretation results that modify validation rules.
 * The payload T is a single interpreted value which most interpretations use.
 * For more complex results subclass and add more properties.
 */
public class InterpretationResult<T> {
  private final T payload;
  private final Map<OccurrenceValidationRule, Boolean> rules = Maps.newHashMap();

  public InterpretationResult(T payload) {
    this.payload = payload;
  }

  public T getPayload() {
    return payload;
  }

  public void setValidationRule(OccurrenceValidationRule rule, boolean result) {
    Preconditions.checkNotNull("Validation rule must be specified", rule);
    rules.put(rule, result);
  }

  public Map<OccurrenceValidationRule, Boolean> getValidationRules() {
    return rules;
  }


  @Override
  public int hashCode() {
    return Objects.hashCode(payload, rules);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final InterpretationResult other = (InterpretationResult) obj;
    return Objects.equal(this.payload, other.payload)
           && Objects.equal(this.rules, other.rules);
  }
}
