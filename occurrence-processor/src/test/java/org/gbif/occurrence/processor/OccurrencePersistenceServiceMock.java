package org.gbif.occurrence.processor;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.occurrence.persistence.hbase.FieldName;
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
  private final Map<Integer, Occurrence> cache = Maps.newHashMap();
  private final Map<Integer, VerbatimOccurrence> vCache = Maps.newHashMap();



  public OccurrencePersistenceServiceMock(FragmentPersistenceService fragService) {
    this.fragService = fragService;
  }

  @Override
  public Occurrence get(Integer key) {
    return cache.get(key);
  }

  @Nullable
  @Override
  public VerbatimOccurrence getVerbatim(Integer key) {
    return vCache.get(key);
  }

  @Override
  public String getFragment(int key) {
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
  public Occurrence delete(int occurrenceKey) {
    return cache.remove(occurrenceKey);
  }

  @Override
  public void delete(List<Integer> occurrenceKeys) {
    for (Integer key : occurrenceKeys) {
      delete(key);
    }
  }

  @Override
  public Iterator<Integer> getKeysByColumn(byte[] columnValue, FieldName columnName) {
    // TODO: Write implementation
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
