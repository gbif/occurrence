package org.gbif.occurrence.download.file.common;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.gbif.common.search.solr.SolrConstants;
import org.gbif.occurrence.download.file.DownloadFileWork;

import java.io.IOException;

/**
 * Executes a Solr query and applies a predicate to each result.
 */
public class SolrQueryProcessor {

  // Default page size for Solr queries.
  private static final int LIMIT = 300;

  private static final String KEY_FIELD = "";//OccurrenceSolrField.KEY.getFieldName();
  /**
   * Executes a query on the SolrServer parameter and applies the predicate to each result.
   *
   * @param downloadFileWork it's used to determine how to page through the results and the Solr query to be used
   * @param resultHandler    predicate that process each result, receives as parameter the occurrence key
   */
  public static void processQuery(DownloadFileWork downloadFileWork, Predicate<Integer> resultHandler) {

    // Calculates the amount of output records
    int nrOfOutputRecords = downloadFileWork.getTo() - downloadFileWork.getFrom();

    // Creates a search request instance using the search request that comes in the fileJob
    SolrQuery solrQuery = createSolrQuery(downloadFileWork.getQuery());
    //key is required since this runs in a distributed installations where the natural order can't be guaranteed
    solrQuery.setSort(KEY_FIELD, SolrQuery.ORDER.desc);

    try {
      int recordCount = 0;
      while (recordCount < nrOfOutputRecords) {
        solrQuery.setStart(downloadFileWork.getFrom() + recordCount);
        // Limit can't be greater than the maximum number of records assigned to this job
        solrQuery.setRows(recordCount + LIMIT > nrOfOutputRecords ? nrOfOutputRecords - recordCount : LIMIT);
        QueryResponse response = downloadFileWork.getSolrClient().query(solrQuery);
        for (SolrDocument solrDocument : response.getResults()) {
          resultHandler.apply((Integer) solrDocument.getFieldValue(KEY_FIELD));
        }
        recordCount += response.getResults().size();
      }
    } catch (SolrServerException | IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a SolrQuery that contains the query parameter as the filter query value.
   */
  private static SolrQuery createSolrQuery(String query) {
    SolrQuery solrQuery = new SolrQuery();
    solrQuery.setQuery(SolrConstants.DEFAULT_QUERY);
    if (!Strings.isNullOrEmpty(query)) {
      solrQuery.addFilterQuery(query);
    }
    return solrQuery;
  }

  /**
   * Hidden constructor.
   */
  private SolrQueryProcessor() {
    //empty constructor
  }
}
