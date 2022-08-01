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
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.query.QueryVisitorsFactory;
import org.gbif.occurrence.search.es.EsFieldMapper;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.lang3.StringEscapeUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
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

  private static final String SEARCH_QUERY = "search_query";

  private static final String HIVE_DB = "hive_db";

  private static final String HIVE_QUERY = "hive_query";

  private static final String DOWNLOAD_KEY = "download_key";


  //'-' is not allowed in a Hive table name.
  // This value will hold the same value as the DOWNLOAD_KEY but the - is replaced by an '_'.
  private static final String DOWNLOAD_TABLE_NAME = "download_table_name";

  private final RestHighLevelClient esClient;

  private final String esIndex;

  private final EsFieldMapper esFieldMapper;

  // Holds the value of the maximum number of records that a small download can have.
  private final int smallDownloadLimit;

  private final OccurrenceDownloadService occurrenceDownloadService;

  private final WorkflowConfiguration workflowConfiguration;

  private final DwcTerm coreTerm;

  /**
   * Entry point: receives as argument the predicate filter and the Oozie workflow id.
   */
  public static void main(String[] args) throws Exception {
    checkArgument(args.length > 0 && !Strings.isNullOrEmpty(args[0]), "The search query argument hasn't been specified");
    try (DownloadPrepareAction occurrenceCount = DownloadWorkflowModule.builder()
                                                  .workflowConfiguration(new WorkflowConfiguration())
                                                  .build()
                                                    .downloadPrepareAction(DwcTerm.valueOf(args[3]))) {
      occurrenceCount.updateDownloadData(args[0], DownloadUtils.workflowToDownloadId(args[1]), args[2]);
    }
  }




  /**
   * Method that determines if the search query produces a "small" download file.
   */
  public Boolean isSmallDownloadCount(long recordCount) {
    return recordCount != ERROR_COUNT && recordCount <= smallDownloadLimit && DwcTerm.Occurrence == coreTerm;
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
      props.setProperty(DOWNLOAD_KEY, downloadKey);
      // '-' is replaced by '_' because it's not allowed in hive table names
      props.setProperty(DOWNLOAD_TABLE_NAME, downloadKey.replaceAll("-", "_"));
      props.setProperty(HIVE_DB, workflowConfiguration.getHiveDb());

      Predicate predicate = OBJECT_MAPPER.readValue(rawPredicate, Predicate.class);
      String searchQuery = QueryVisitorsFactory.createEsQueryVisitor(workflowConfiguration.getEsIndexType()).buildQuery(predicate);
      long recordCount = getRecordCount(searchQuery);
      props.setProperty(IS_SMALL_DOWNLOAD, isSmallDownloadCount(recordCount).toString());
      if (isSmallDownloadCount(recordCount)) {
        props.setProperty(SEARCH_QUERY, StringEscapeUtils.escapeXml10(searchQuery));
      }
      props.setProperty(HIVE_QUERY, StringEscapeUtils.escapeXml10(QueryVisitorsFactory.createSqlQueryVisitor().buildQuery(predicate)));
      if (recordCount >= 0 && DownloadFormat.valueOf(downloadFormat.trim()) != DownloadFormat.SPECIES_LIST) {
        updateTotalRecordsCount(downloadKey, recordCount);
      }

      persist(oozieProp, props);
    } else {
      throw new IllegalStateException(OOZIE_ACTION_OUTPUT_PROPERTIES + " System property not defined");
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
      if(Objects.nonNull(esClient)) {
        esClient.close();
      }
    } catch (IOException ex) {
      LOG.error("Error shutting down Elasticsearch client", ex);
    }
  }

  /**
   * Updates the record count of the download entity.
   */
  private void updateTotalRecordsCount(String downloadKey, long recordCount) {
    try {
      LOG.info("Updating record count({}) of download {}", recordCount, downloadKey);
      Download download = occurrenceDownloadService.get(downloadKey);
      if (download == null) {
        LOG.error("Download {} was not found!", downloadKey);
      } else {
        download.setTotalRecords(recordCount);
        occurrenceDownloadService.update(download);
      }
    } catch (Exception ex) {
      LOG.error("Error updating record count for download workflow {}, reported count is {}", downloadKey, recordCount, ex);
    }
  }

  @Override
  public void close() {
    shutDownEsClientSilently();
  }

}
