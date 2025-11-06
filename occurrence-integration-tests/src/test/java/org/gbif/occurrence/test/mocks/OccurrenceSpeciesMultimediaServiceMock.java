package org.gbif.occurrence.test.mocks;

import org.gbif.occurrence.persistence.OccurrenceSpeciesMultimediaService;

public class OccurrenceSpeciesMultimediaServiceMock implements OccurrenceSpeciesMultimediaService {
  @Override
  public TaxonMultimediaSearchResponse queryMediaInfo(String taxonKey, String mediaType, int limitRequest, int offset) {
    return null;
  }
}
