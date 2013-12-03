package org.gbif.occurrence.ws.client.mock;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.service.occurrence.VerbatimOccurrenceService;

public class VerbatimOccurrencePersistenceMockService implements VerbatimOccurrenceService {

  @Override
  public VerbatimOccurrence getVerbatim(Integer key) {

    return new VerbatimOccurrence();
  }
}
