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
package org.gbif.metrics.ws.service;

import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.service.occurrence.OccurrenceSearchService;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Occurrence metrics backed by occurrence-search with cache2k.
 */
@Service
public class CachedOccurrenceMetricsService implements OccurrenceMetricsService {

  private static final Logger LOG = LoggerFactory.getLogger(CachedOccurrenceMetricsService.class);

  private static final int FACET_PAGE_SIZE = 30_000;

  /** Dimension (cache key prefix) → facet parameter for occurrence-search. */
  private static final Map<String, OccurrenceSearchParameter> DIMENSION_TO_FACET_PARAM;

  static {
    DIMENSION_TO_FACET_PARAM = Map.of(
      "basisOfRecord", OccurrenceSearchParameter.BASIS_OF_RECORD,
      "countries", OccurrenceSearchParameter.COUNTRY,
      "datasets", OccurrenceSearchParameter.DATASET_KEY,
      "kingdom", OccurrenceSearchParameter.KINGDOM_KEY,
      "publishingCountries", OccurrenceSearchParameter.PUBLISHING_COUNTRY,
      "year", OccurrenceSearchParameter.YEAR);
  }

  private final OccurrenceSearchService searchService;
  private final Cache<Predicate, Long> countCache;
  private final Cache<InventoryCacheKey, Map<String, Long>> inventoryCache;

  public CachedOccurrenceMetricsService(
      OccurrenceSearchService searchService,
      CacheConfig cacheConfig) {
    this.searchService = searchService;
    this.countCache =
        new Cache2kBuilder<Predicate, Long>() {}
            .loader(this::loadCount)
            .expireAfterWrite(cacheConfig.getExpireAfterWrite(), TimeUnit.MILLISECONDS)
            .refreshAhead(cacheConfig.isRefreshAhead())
            .entryCapacity(cacheConfig.getEntryCapacity())
            .build();
    this.inventoryCache =
        new Cache2kBuilder<InventoryCacheKey, Map<String, Long>>() {}
            .loader(this::loadInventory)
            .expireAfterWrite(cacheConfig.getExpireAfterWrite(), TimeUnit.MILLISECONDS)
            .refreshAhead(cacheConfig.isRefreshAhead())
            .entryCapacity(cacheConfig.getEntryCapacity())
            .build();
  }

  @Override
  public long count(Predicate predicate) {
    if (predicate == null) {
      return searchService.countRecords(null);
    }
    return countCache.get(predicate);
  }

  private Long loadCount(Predicate predicate) {
    return searchService.countRecords(predicate);
  }

  @Override
  public Map<String, Long> getBasisOfRecordCounts() {
    return inventoryCache.get(InventoryCacheKey.basisOfRecord());
  }

  @Override
  public Map<String, Long> getCountries(String publishingCountry) {
    return inventoryCache.get(InventoryCacheKey.countries(publishingCountry));
  }

  @Override
  public Map<String, Long> getDatasets(String country, Integer nubKey, String taxonKey, String checklistKey) {
    return inventoryCache.get(InventoryCacheKey.datasets(country, nubKey, taxonKey, checklistKey));
  }

  @Override
  public Map<String, Long> getKingdomCounts(String checklistKey) {
    return inventoryCache.get(InventoryCacheKey.kingdom(checklistKey));
  }

  @Override
  public Map<String, Long> getPublishingCountries(String country) {
    return inventoryCache.get(InventoryCacheKey.publishingCountries(country));
  }

  @Override
  public Map<String, Long> getYearCounts(String year) {
    return inventoryCache.get(InventoryCacheKey.year(year));
  }

  private Map<String, Long> loadInventory(InventoryCacheKey key) {
    OccurrenceSearchParameter facetParam = DIMENSION_TO_FACET_PARAM.get(key.dimension());
    if (facetParam == null) {
      LOG.warn("Unknown inventory dimension: {}", key.dimension());
      return Collections.emptyMap();
    }
    java.util.function.Consumer<OccurrenceSearchRequest> filter = buildFilter(key);
    return runFacetQuery(facetParam, filter);
  }

  private java.util.function.Consumer<OccurrenceSearchRequest> buildFilter(InventoryCacheKey key) {
    return switch (key.dimension()) {
      case "basisOfRecord" -> null;
      case "countries" -> req -> {
        if (key.publishingCountry() != null && !key.publishingCountry().isEmpty()) {
          req.addParameter(OccurrenceSearchParameter.PUBLISHING_COUNTRY,
            key.publishingCountry());
        }
      };
      case "datasets" -> req -> {
        if (key.country() != null)
          req.addParameter(OccurrenceSearchParameter.COUNTRY, key.country());
        if (key.taxonKey() != null)
          req.addParameter(OccurrenceSearchParameter.TAXON_KEY, key.taxonKey());
        if (key.nubKey() != null && key.taxonKey() == null) {
          req.addParameter(OccurrenceSearchParameter.TAXON_KEY, key.nubKey().toString());
        }
        if (key.checklistKey() != null) {
          req.addParameter(OccurrenceSearchParameter.CHECKLIST_KEY, key.checklistKey());
        }
      };
      case "kingdom" -> req -> {
        if (key.checklistKey() != null && !key.checklistKey().isEmpty()) {
          req.addParameter(OccurrenceSearchParameter.CHECKLIST_KEY, key.checklistKey());
        }
      };
      case "publishingCountries" -> req -> {
        if (key.country() != null && !key.country().isEmpty()) {
          req.addParameter(OccurrenceSearchParameter.COUNTRY, key.country());
        }
      };
      case "year" -> req -> {
        if (key.year() != null && !key.year().isEmpty()) {
          req.addParameter(OccurrenceSearchParameter.YEAR, key.year());
        }
      };
      default -> null;
    };
  }

  /**
     * Type-safe cache key for inventory (facet) queries. Replaces string concatenation and avoids
     * delimiter/parsing issues.
     */
    private record InventoryCacheKey(String dimension, String publishingCountry, String country,
                                     Integer nubKey, String taxonKey, String checklistKey, String year) {

    static InventoryCacheKey basisOfRecord() {
        return new InventoryCacheKey("basisOfRecord", null, null, null, null, null, null);
      }

      static InventoryCacheKey countries(String publishingCountry) {
        return new InventoryCacheKey("countries", publishingCountry, null, null, null, null, null);
      }

      static InventoryCacheKey datasets(String country, Integer nubKey, String taxonKey, String checklistKey) {
        return new InventoryCacheKey("datasets", null, country, nubKey, taxonKey, checklistKey, null);
      }

      static InventoryCacheKey kingdom(String checklistKey) {
        return new InventoryCacheKey("kingdom", null, null, null, null, checklistKey, null);
      }

      static InventoryCacheKey publishingCountries(String country) {
        return new InventoryCacheKey("publishingCountries", null, country, null, null, null, null);
      }

      static InventoryCacheKey year(String year) {
        return new InventoryCacheKey("year", null, null, null, null, null, year);
      }

  }

  private Map<String, Long> runFacetQuery(
      OccurrenceSearchParameter facetParam,
      java.util.function.Consumer<OccurrenceSearchRequest> filter) {
    OccurrenceSearchRequest req = new OccurrenceSearchRequest();
    req.setLimit(0);
    req.addFacets(facetParam);
    req.addFacetPage(facetParam, 0, FACET_PAGE_SIZE);
    if (filter != null) {
      filter.accept(req);
    }
    SearchResponse<Occurrence, OccurrenceSearchParameter> response = searchService.search(req);
    if (response.getFacets() == null || response.getFacets().isEmpty()) {
      return Collections.emptyMap();
    }
    return response.getFacets().stream()
        .filter(f -> f.getField() == facetParam)
        .findFirst()
        .map(this::facetCountsToMap)
        .orElse(Collections.emptyMap());
  }

  private Map<String, Long> facetCountsToMap(Facet<OccurrenceSearchParameter> facet) {
    Map<String, Long> map = new LinkedHashMap<>();
    for (Facet.Count c : facet.getCounts()) {
      map.put(c.getName(), c.getCount());
    }
    return map;
  }
}
