package org.gbif.occurrence.download.file.common;

import org.gbif.common.search.util.SolrConstants;
import org.gbif.occurrence.download.file.FileJob;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job that creates a part of CSV file. The file is generated according to the fileJob field.
 */
public class SolrQueryProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(SolrQueryProcessor.class);

  // Default page size for Solr queries.
  private static final int LIMIT = 300;


  /**
   * Executes the job.query and creates a data file that will contains the records from job.from to job.to positions.
   */
  public static void processQuery(FileJob fileJob, SolrServer solrServer, Predicate<Integer> resultHandler) throws IOException {

    // Calculates the amount of output records
    final int nrOfOutputRecords = fileJob.getTo() - fileJob.getFrom();

    // Creates a search request instance using the search request that comes in the fileJob
    SolrQuery solrQuery = createSolrQuery(fileJob.getQuery());

    try {
      int recordCount = 0;
      while (recordCount < nrOfOutputRecords) {
        solrQuery.setStart(fileJob.getFrom() + recordCount);
        // Limit can't be greater than the maximum number of records assigned to this job
        solrQuery.setRows(recordCount + LIMIT > nrOfOutputRecords ? nrOfOutputRecords - recordCount : LIMIT);
        final QueryResponse response = solrServer.query(solrQuery);
        for (Iterator<SolrDocument> itResults = response.getResults().iterator(); itResults.hasNext(); recordCount++) {
          resultHandler.apply((Integer) itResults.next().getFieldValue(OccurrenceSolrField.KEY.getFieldName()));
        }
      }
    } catch (SolrServerException e) {
      Throwables.propagate(e);
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

}
