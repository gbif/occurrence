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
package org.gbif.occurrence.test.mocks;

import org.gbif.api.model.common.DOI;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadStatistics;
import org.gbif.api.model.registry.CountryOccurrenceDownloadUsage;
import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.api.model.registry.OrganizationOccurrenceDownloadUsage;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.Country;

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.RandomStringUtils;

import org.gbif.api.vocabulary.CountryUsageSortField;
import org.gbif.api.vocabulary.DatasetUsageSortField;
import org.gbif.api.vocabulary.OrganizationUsageSortField;
import org.gbif.api.vocabulary.SortOrder;

public class OccurrenceDownloadServiceMock implements OccurrenceDownloadService {

  private Map<String, Download> downloads = new HashMap<>();

  private Map<String, List<DatasetOccurrenceDownloadUsage>> usages = new HashMap<>();

  private static final String DOI_PREFIX = "10.5072";

  private DOI randomDoi() {
    String suffix = "dl." + RandomStringUtils.random(6, "23456789abcdefghjkmnpqrstuvwxyz");
    return new DOI(DOI_PREFIX, suffix);
  }

  @Override
  public void create(@NotNull @Valid Download download) {
    String key = UUID.randomUUID().toString();
    download.setKey(key);
    download.setDoi(randomDoi());
    download.setStatus(Download.Status.SUCCEEDED);
    downloads.put(key, download);
  }

  @Override
  public Download get(@NotNull String key) {
    return downloads.get(key);
  }

  private PagingResponse<Download> filterDownloads(Pageable pageable, Predicate<Download> filter) {
    PagingResponse<Download> response = new PagingResponse<>();
    Stream<Download> downloadStream = downloads.values().stream();

    downloadStream = downloadStream.filter(filter);

    if (Objects.nonNull(pageable)) {
      response.setLimit(pageable.getLimit());
      response.setOffset(pageable.getOffset());
      downloadStream = downloadStream.skip(pageable.getOffset()).limit(pageable.getLimit());
    }

    List<Download> results = downloadStream.collect(Collectors.toList());

    response.setCount((long) results.size());
    response.setResults(results);
    return response;
  }

  @Override
  public PagingResponse<Download> list(
      @Nullable Pageable pageable,
      @Nullable Set<Download.Status> statuses,
      @Nullable String source) {
    return filterDownloads(
        pageable,
        d -> statuses.contains(d.getStatus()) && (source == null || d.getSource().equals(source)));
  }

  @Override
  public long count(
      @Nullable Set<Download.Status> set,
      @Nullable String s) {
    return 0;
  }

  @Override
  public PagingResponse<Download> listByUser(
      @NotNull String user,
      @Nullable Pageable pageable,
      @Nullable Set<Download.Status> statuses,
      @Nullable LocalDateTime from,
      @Nullable Boolean statistics) {
    return filterDownloads(
        pageable,
        d -> user.equals(d.getRequest().getCreator()) && statuses.contains(d.getStatus()));
  }

  @Override
  public long countByUser(
      @NotNull String s, @Nullable Set<Download.Status> set, LocalDateTime date) {
    return 0;
  }

  @Override
  public void update(@NotNull @Valid Download download) {
    downloads.replace(download.getKey(), download);
  }

  @Override
  public PagingResponse<DatasetOccurrenceDownloadUsage> listDatasetUsages(
      @NotNull String downloadKey, @Nullable Pageable pageable) {
    PagingResponse<DatasetOccurrenceDownloadUsage> response = new PagingResponse<>(pageable);
    List<DatasetOccurrenceDownloadUsage> downloadUsages =
        usages.get(downloadKey).stream()
            .skip(pageable.getOffset())
            .limit(pageable.getLimit())
            .collect(Collectors.toList());
    response.setCount((long) downloadUsages.size());
    response.setResults(downloadUsages);
    return response;
  }

  @Override
  public PagingResponse<DatasetOccurrenceDownloadUsage> listDatasetUsages(
      @NotNull String s,
      @Nullable String s1,
      @Nullable DatasetUsageSortField datasetUsageSortField,
      @Nullable SortOrder sortOrder,
      @Nullable Pageable pageable) {
    return null;
  }

  @Override
  public PagingResponse<OrganizationOccurrenceDownloadUsage> listOrganizationUsages(
      @NotNull String s,
      @Nullable String s1,
      @Nullable OrganizationUsageSortField organizationUsageSortField,
      @Nullable SortOrder sortOrder,
      @Nullable Pageable pageable) {
    return null;
  }

  @Override
  public PagingResponse<CountryOccurrenceDownloadUsage> listCountryUsages(
      @NotNull String s,
      @Nullable CountryUsageSortField countryUsageSortField,
      @Nullable SortOrder sortOrder,
      @Nullable Pageable pageable) {
    return null;
  }

  @Override
  public String getCitation(@NotNull String downloadKey) {
    Download download = downloads.get(downloadKey);
    return "GBIF Occurrence Download " + download.getDoi().getUrl().toString() + '\n';
  }

  @Override
  public Map<Integer, Map<Integer, Long>> getDownloadsByUserCountry(
      @Nullable Date fromDate, @Nullable Date toDate, @Nullable Country country) {
    return null;
  }

  @Override
  public Map<Integer, Map<Integer, Long>> getDownloadsBySource(
      @Nullable Date fromDate, @Nullable Date toDate, @Nullable String source) {
    return null;
  }

  /**
   * Retrieves downloaded records monthly stats by country (user and publishing country) and
   * dataset.
   */
  @Override
  public Map<Integer, Map<Integer, Long>> getDownloadedRecordsByDataset(
      @Nullable Date fromDate,
      @Nullable Date toDate,
      @Nullable Country publishingCountry,
      @Nullable UUID datasetKey,
      @Nullable UUID publishingOrgKey) {
    return null;
  }

  /** Retrieves downloads monthly stats by country (user and publishing country) and dataset. */
  @Override
  public Map<Integer, Map<Integer, Long>> getDownloadsByDataset(
      @Nullable Date fromDate,
      @Nullable Date toDate,
      @Nullable Country publishingCountry,
      @Nullable UUID datasetKey,
      @Nullable UUID publishingOrgKey) {
    return null;
  }

  /** Retrieves downloads monthly stats by country (user and publishing country) and dataset. */
  @Override
  public PagingResponse<DownloadStatistics> getDownloadStatistics(
      @Nullable Date fromDate,
      @Nullable Date toDate,
      @Nullable Country publishingCountry,
      @Nullable UUID datasetKey,
      @Nullable UUID publishingOrgKey,
      @Nullable Pageable page) {
    return null;
  }

  @Override
  public void createUsages(@NotNull String downloadKey, @NotNull Map<UUID, Long> downloadUsages) {
    usages.put(
        downloadKey,
        downloadUsages.entrySet().stream()
            .map(
                entry -> {
                  UUID datasetKey = entry.getKey();
                  DatasetOccurrenceDownloadUsage datasetOccurrenceDownloadUsage =
                      new DatasetOccurrenceDownloadUsage();
                  datasetOccurrenceDownloadUsage.setDownloadKey(downloadKey);
                  datasetOccurrenceDownloadUsage.setDatasetKey(datasetKey);
                  datasetOccurrenceDownloadUsage.setNumberRecords(entry.getValue());
                  datasetOccurrenceDownloadUsage.setDownload(downloads.get(downloadKey));
                  datasetOccurrenceDownloadUsage.setDatasetCitation(datasetKey.toString());
                  datasetOccurrenceDownloadUsage.setDatasetTitle(datasetKey.toString());
                  return datasetOccurrenceDownloadUsage;
                })
            .collect(Collectors.toList()));
  }

  @Override
  public PagingResponse<Download> listByEraseAfter(
      @Nullable Pageable page,
      @Nullable String eraseAfter,
      @Nullable Long size,
      @Nullable String erasureNotification) {
    return null;
  }
}
