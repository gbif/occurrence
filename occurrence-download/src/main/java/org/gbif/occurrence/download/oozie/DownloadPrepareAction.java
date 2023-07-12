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
package org.gbif.occurrence.download.oozie;

import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.query.QueryBuildingException;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.hive.ExtensionsQuery;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.query.QueryVisitorsFactory;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class sets the following parameters required by the download workflow:
 * - is_small_download: define if the occurrence download must be processed as a small(ES) or a big (Hive) download.
 * This parameter is calculated by executing an ElasticSearch query that counts the number of records.
 * - search_query: query to process small download, it's a translation of the predicate filter.
 * - hive_query: query to process big download, it's a translation of the predicate filter.
 * - hive_db: this parameter is read from a properties file.
 * - download_key: download primary key, it's generated from the Oozie workflow id.
 * - download_table_name: base name to use when creating hive tables and files, it's the download_key, but the '-'
 * is replaced by '_'.
 */
@Data
@Builder
public class  DownloadPrepareAction implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadPrepareAction.class);

  // arbitrary record count that represents and error counting the records of the input query
  private static final int ERROR_COUNT = -1;

  private static final ObjectMapper OBJECT_MAPPER =
    new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  static {
    OBJECT_MAPPER.addMixIn(SearchParameter.class, QueryVisitorsFactory.OccurrenceSearchParameterMixin.class);
  }

  private static final String OOZIE_ACTION_OUTPUT_PROPERTIES = "oozie.action.output.properties";

  private static final String IS_SMALL_DOWNLOAD = "is_small_download";

  private static final String HAS_VERBATIM_EXTENSIONS = "has_verbatim_extensions";

  private static final String SEARCH_QUERY = "search_query";

  private static final String HIVE_DB = "hive_db";

  private static final String HIVE_QUERY = "hive_query";

  private static final String DOWNLOAD_KEY = "download_key";


  //'-' is not allowed in a Hive table name.
  // This value will hold the same value as the DOWNLOAD_KEY but the - is replaced by an '_'.
  private static final String DOWNLOAD_TABLE_NAME = "download_table_name";

  private static final String CORE_TERM_NAME = "core_term_name";

  private static final String SOURCE_TABLE = "table_name";

  private static final String VERBATIM_EXTENSIONS = "verbatim_extensions";

  private final RestHighLevelClient esClient;

  private final String esIndex;

  // Holds the value of the maximum number of records that a small download can have.
  private final int smallDownloadLimit;

  private final OccurrenceDownloadService occurrenceDownloadService;

  private final WorkflowConfiguration workflowConfiguration;

  private final DwcTerm coreTerm;

  private final String wfPath;

  private final OccurrenceBaseEsFieldMapper esFieldMapper;

  /**
   * Entry point: receives as argument the predicate filter and the Oozie workflow id.
   */
  public static void main(String[] args) throws Exception {
    checkArgument(args.length > 3 && !Strings.isNullOrEmpty(args[0]), "The search query argument hasn't been specified");
    try (DownloadPrepareAction occurrenceCount = DownloadWorkflowModule.builder()
                                                  .workflowConfiguration(new WorkflowConfiguration())
                                                  .build()
                                                    .downloadPrepareAction(DwcTerm.valueOf(args[3]), args[4])) {
      occurrenceCount.updateDownloadData(args[0], args[1], args[2]);
    }
  }

  /**
   * Method that determines if the search query produces a "small" download file.
   */
  public Boolean isSmallDownloadCount(long recordCount) {
    return recordCount != ERROR_COUNT && recordCount <= smallDownloadLimit;
  }

  /**
   * Update the Oozie workflow data/parameters and persists the records of the occurrence download.
   *
   * @param rawPredicate to be executed
   * @param downloadKey  workflow id
   *
   * @throws java.io.IOException in case of error reading or writing the 'oozie.action.output.properties' file
   */
  public void updateDownloadData(String rawPredicate, String downloadKey, String downloadFormat)
      throws IOException, QueryBuildingException {

    Properties props = new Properties();
    String oozieProp = System.getProperty(OOZIE_ACTION_OUTPUT_PROPERTIES);

    if (oozieProp != null) {

      Download download = getDownload(downloadKey);

      setRequestExtensionsParam(download, props);
      props.setProperty(DOWNLOAD_KEY, downloadKey);
      // '-' is replaced by '_' because it's not allowed in hive table names
      props.setProperty(DOWNLOAD_TABLE_NAME, DownloadUtils.downloadTableName(downloadKey));
      props.setProperty(HIVE_DB, workflowConfiguration.getHiveDb());
      props.setProperty(CORE_TERM_NAME, coreTerm.name());
      props.setProperty(SOURCE_TABLE, coreTerm.name().toLowerCase());

      Predicate predicate = OBJECT_MAPPER.readValue(rawPredicate, Predicate.class);
      String searchQuery = searchQuery(predicate);
      long recordCount = getRecordCount(searchQuery);
      props.setProperty(IS_SMALL_DOWNLOAD, isSmallDownloadCount(recordCount).toString());
      if (isSmallDownloadCount(recordCount)) {
        props.setProperty(SEARCH_QUERY, StringEscapeUtils.escapeXml10(searchQuery));
      }
      props.setProperty(HIVE_QUERY, StringEscapeUtils.escapeXml10(QueryVisitorsFactory.createSqlQueryVisitor().buildQuery(predicate)));
      if (recordCount >= 0 && DownloadFormat.valueOf(downloadFormat.trim()) != DownloadFormat.SPECIES_LIST) {
        updateTotalRecordsCount(download, recordCount);
      }

      persist(oozieProp, props);
    } else {
      throw new IllegalStateException(OOZIE_ACTION_OUTPUT_PROPERTIES + " System property not defined");
    }

  }

  @SneakyThrows
  private String searchQuery(Predicate predicate) {
    Optional<QueryBuilder> queryBuilder = QueryVisitorsFactory.createEsQueryVisitor(esFieldMapper)
                                            .getQueryBuilder(predicate);
    if (queryBuilder.isPresent()) {
      BoolQueryBuilder query = (BoolQueryBuilder) queryBuilder.get();
      esFieldMapper.getDefaultFilter().ifPresent(df -> query.filter().add(df));
      return query.toString();
    }
    return esFieldMapper.getDefaultFilter().orElse(QueryBuilders.matchAllQuery()).toString();
  }

  /**
   * Gets a download by its key.
   */
  private Download getDownload(String downloadKey) {
    Download download = occurrenceDownloadService.get(downloadKey);
    if (download == null) {
      LOG.error("Download {} not found!", downloadKey);
    }
    return download;
  }

  @SneakyThrows
  private FileSystem getHadoopFileSystem() {
    Configuration configuration = new Configuration();
    return FileSystem.get(configuration);
  }

  @SneakyThrows
  private void generateExtensionQueryFile(Download download) {
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(getHadoopFileSystem().create(new Path(wfPath + "/tmp/",
                                                                                                                   DownloadUtils.downloadTableName(download.getKey()) + "-execute-extensions-query.q"))))) {
      ExtensionsQuery.builder().writer(writer).build().generateExtensionsQueryHQL(download);
    }
  }

  /**
   * Sets the extensions parameter.
   */
  private void setRequestExtensionsParam(Download download, Properties props) {
    if (download != null && download.getRequest().getVerbatimExtensions() != null && !download.getRequest().getVerbatimExtensions().isEmpty()) {
      String requestExtensions = Optional.ofNullable(download.getRequest().getVerbatimExtensions())
        .map(verbatimExtensions -> verbatimExtensions.stream().map(Enum::name).collect(Collectors.joining(",")))
        .orElse("");
      props.setProperty(VERBATIM_EXTENSIONS, requestExtensions);
      props.setProperty(HAS_VERBATIM_EXTENSIONS, Boolean.TRUE.toString());
      generateExtensionQueryFile(download);
    } else {
      props.setProperty(HAS_VERBATIM_EXTENSIONS, Boolean.FALSE.toString());
    }
  }

  private void persist(String propPath, Properties properties) throws IOException {
    try (OutputStream os = new FileOutputStream(propPath)) {
      properties.store(os, "");
    } catch (FileNotFoundException e) {
      LOG.error("Error reading properties file", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Executes the ElasticSearch query and returns the number of records found.
   * If an error occurs 'ERROR_COUNT' is returned.
   */
  private long getRecordCount(String esQuery) {
    try {
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).trackTotalHits(true);
      if(!Strings.isNullOrEmpty(esQuery)) {
        searchSourceBuilder.query(QueryBuilders.wrapperQuery(esQuery));
      }
      SearchResponse response = esClient.search(new SearchRequest().indices(esIndex).source(searchSourceBuilder), RequestOptions.DEFAULT);
      long count = response.getHits().getTotalHits().value;
      LOG.info("Download record count {}", count);
      return count;
    } catch (Exception e) {
      LOG.error("Error getting the records count", e);
      return ERROR_COUNT;
    }
  }

  /**
   * Shuts down the ElasticSearch client.
   */
  private void shutDownEsClientSilently() {
    try {
      if (Objects.nonNull(esClient)) {
        esClient.close();
      }
    } catch (IOException ex) {
      LOG.error("Error shutting down Elasticsearch client", ex);
    }
  }

  /**
   * Updates the record count of the download entity.
   */
  private void updateTotalRecordsCount(Download download, long recordCount) {
    try {
      if (download != null) {
        LOG.info("Updating record count({}) of download {}", recordCount, download);
        download.setTotalRecords(recordCount);
        occurrenceDownloadService.update(download);
      }
    } catch (Exception ex) {
      LOG.error("Error updating record count for download workflow , reported count is {}", recordCount, ex);
    }
  }

  @Override
  public void close() {
    shutDownEsClientSilently();
  }

}
