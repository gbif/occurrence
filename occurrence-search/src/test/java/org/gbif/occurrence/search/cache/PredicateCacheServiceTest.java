package org.gbif.occurrence.search.cache;

import org.gbif.api.model.occurrence.predicate.IsNullPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import org.cache2k.config.Cache2kConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for the default PredicateCacheService.*/
public class PredicateCacheServiceTest {

  @Test
  public void putAndGetTest() {
    //Cache config
    Cache2kConfig<Integer,Predicate> cache2kConfig = new Cache2kConfig<>();
    cache2kConfig.setEntryCapacity(1);
    cache2kConfig.setPermitNullValues(true);

    PredicateCacheService predicateCacheService = DefaultInMemoryPredicateCacheService.getInstance(cache2kConfig);
    IsNullPredicate isNullPredicate = new IsNullPredicate(OccurrenceSearchParameter.BASIS_OF_RECORD);

    //Store the predicate
    Integer predicateHash = predicateCacheService.put(isNullPredicate);

    //Compare hashes
    assertEquals(predicateHash, predicateCacheService.hash(isNullPredicate));

    //Compare predicates
    assertEquals(isNullPredicate, predicateCacheService.get(predicateHash));
  }
}
