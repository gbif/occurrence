package org.gbif.occurrence.processor;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import com.google.common.collect.Maps;

public class OccurrencePersistenceServiceMock implements OccurrencePersistenceService {

  private final FragmentPersistenceService fragService;
  private final Map<Long, Occurrence> cache = Maps.newHashMap();
  private final Map<Long, VerbatimOccurrence> vCache = Maps.newHashMap();



  public OccurrencePersistenceServiceMock(FragmentPersistenceService fragService) {
    this.fragService = fragService;
  }

  @Override
  public Occurrence get(Long key) {
    return cache.get(key);
  }

  @Nullable
  @Override
  public VerbatimOccurrence getVerbatim(Long key) {
    return vCache.get(key);
  }

  @Override
  public String getFragment(long key) {
    Fragment frag = fragService.get(key);
    return frag == null ? null : String.valueOf(frag.getData());
  }

  @Override
  public void update(Occurrence occurrence) {
    cache.put(occurrence.getKey(), occurrence);
  }

  @Override
  public void update(VerbatimOccurrence occurrence) {
    vCache.put(occurrence.getKey(), occurrence);
  }

  @Override
  public Occurrence delete(long occurrenceKey) {
    return cache.remove(occurrenceKey);
  }

  @Override
  public void delete(List<Long> occurrenceKeys) {
    for (Long key : occurrenceKeys) {
      delete(key);
    }
  }

  @Override
  public Iterator<Long> getKeysByColumn(byte[] columnValue, String columnName) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
