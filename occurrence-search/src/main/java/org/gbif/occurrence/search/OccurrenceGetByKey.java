package org.gbif.occurrence.search;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;

public interface OccurrenceGetByKey {

  Occurrence get(Long key);

  VerbatimOccurrence getVerbatim(Long key);
}
