package org.gbif.occurrencestore.processor;

import org.gbif.occurrencestore.persistence.api.VerbatimOccurrence;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrencePersistenceService;

import java.util.Map;

import com.google.common.collect.Maps;


public class VerbatimOccurrenceServiceMock implements VerbatimOccurrencePersistenceService {

  private final Map<Integer, VerbatimOccurrence> cache = Maps.newHashMap();

  @Override
  public VerbatimOccurrence get(Integer key) {
    return cache.get(key);
  }

  @Override
  public void update(VerbatimOccurrence occurrence) {
    cache.put(occurrence.getKey(), occurrence);
  }
}
