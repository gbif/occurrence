package org.gbif.occurrence.search.cache;

import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.cache2k.Cache;
import org.cache2k.config.Cache2kConfig;

/** Default implementation, it uses an in-memory Cache2k cache that can be configured externally. */
public class DefaultInMemoryPredicateCacheService implements PredicateCacheService {

  private final Cache<Integer, Predicate> cache;

  private final ObjectMapper mapper;

  public DefaultInMemoryPredicateCacheService(ObjectMapper mapper, Cache<Integer, Predicate> cache) {
    this.mapper = mapper;
    this.cache = cache;
  }

  /** Factory method: creates a Cache using the default jackson mapper and the cache config provided.*/
  public static PredicateCacheService getInstance(Cache2kConfig<Integer,Predicate> cache2kConfig) {
    return new DefaultInMemoryPredicateCacheService(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport(),
                                                    cache2kConfig.builder().build());
  }

  @Override
  public int hash(Predicate predicate) {
    return predicate != null? mapper.valueToTree(predicate).hashCode() : 0;
  }

  @Override
  public Predicate get(int hash) {
    return cache.peek(hash);
  }

  @Override
  public int put(Predicate predicate) {
    int hash  = hash(predicate);
    cache.putIfAbsent(hash, predicate);
    return hash;
  }
}
