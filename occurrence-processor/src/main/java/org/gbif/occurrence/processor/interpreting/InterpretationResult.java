package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class InterpretationResult {

  private final Occurrence original;
  private final Occurrence updated;

  public InterpretationResult(@Nullable Occurrence original, Occurrence updated) {
    this.original = original;
    this.updated = checkNotNull(updated, "updated can't be null");
  }

  public Occurrence getOriginal() {
    return original;
  }

  public Occurrence getUpdated() {
    return updated;
  }
}
