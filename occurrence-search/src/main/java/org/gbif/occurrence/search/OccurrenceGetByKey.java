package org.gbif.occurrence.search;

import org.gbif.api.model.occurrence.Occurrence;

public interface OccurrenceGetByKey {

  Occurrence get(long key);
}
