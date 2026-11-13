/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.search.cache;

import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.rest.client.species.Metadata;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.gbif.rest.client.species.NameUsageMatchingService;

import java.time.Duration;
import java.util.Optional;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Caches the checklist metadata, which changes rarely but is read several times for each search
 * request. Matches are not cached.
 */
public class CachingNameUsageMatchingService implements NameUsageMatchingService {

  // Keys come from requests, so the size is bounded
  private static final long MAX_CHECKLISTS = 100;
  private static final Duration EXPIRY = Duration.ofHours(1);

  private final NameUsageMatchingService delegate;
  private final LoadingCache<String, Optional<Metadata>> metadataCache;

  public CachingNameUsageMatchingService(NameUsageMatchingService delegate) {
    this.delegate = delegate;
    this.metadataCache =
        CacheBuilder.newBuilder()
            .maximumSize(MAX_CHECKLISTS)
            .expireAfterWrite(EXPIRY)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Optional<Metadata> load(String checklistKey) {
                    return Optional.ofNullable(delegate.getMetadata(checklistKey));
                  }
                });
  }

  @Override
  public NameUsageMatchResponse match(NameUsageMatchRequest request) {
    return delegate.match(request);
  }

  @Override
  public Metadata getMetadata(String checklistKey) {
    if (checklistKey == null) {
      return delegate.getMetadata(null);
    }
    try {
      return metadataCache.getUnchecked(checklistKey).orElse(null);
    } catch (UncheckedExecutionException e) {
      // keep the client's exception, e.g. for an unknown checklist
      if (e.getCause() instanceof RuntimeException cause) {
        throw cause;
      }
      throw e;
    }
  }
}
