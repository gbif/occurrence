package org.gbif.occurrence.download.file;

import org.gbif.common.search.util.SolrConstants;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.download.util.HeadersFileUtil;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;
import org.gbif.wrangler.lock.Lock;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

import static org.gbif.occurrence.common.download.HiveFieldUtil.getHiveField;

/**
 * Job that creates a part of CSV file. The file is generated according to the fileJob field.
 */
class OccurrenceFileWriterJob implements Callable<Result> {


  public static final String[] HEADER = HeadersFileUtil.getColumnHeaders();

  // Default page size for Solr queries.
  private static final int LIMIT = 300;

  private final FileJob fileJob;

  private final Lock lock;

  private final SolrServer solrServer;

  private final OccurrenceMapReader occurrenceHBaseReader;

  /**
   * Default constructor.
   */
  public OccurrenceFileWriterJob(FileJob fileJob, Lock lock, SolrServer solrServer,
    OccurrenceMapReader occurrenceHBaseReader) {
    this.fileJob = fileJob;
    this.lock = lock;
    this.solrServer = solrServer;
    this.occurrenceHBaseReader = occurrenceHBaseReader;
  }

  /**
   * Executes the job.query and creates a data file that will contains the records from job.from to job.to positions.
   */
  @Override
  public Result call() throws IOException {
    // Calculates the amount of output records
    final int nrOfOutputRecords = fileJob.getTo() - fileJob.getFrom();
    Map<UUID, Long> datasetUsages = Maps.newHashMap();

    // Creates a search request instance using the search request that comes in the fileJob
    SolrQuery solrQuery = createSolrQuery(fileJob.getQuery());

    Closer closer = Closer.create();
    try {
      ICsvMapWriter csvWriter =
        new CsvMapWriter(new FileWriterWithEncoding(fileJob.getDataFile(), Charsets.UTF_8),
          CsvPreference.TAB_PREFERENCE);
      closer.register(csvWriter);
      int recordCount = 0;
      while (recordCount < nrOfOutputRecords) {
        solrQuery.setStart(fileJob.getFrom() + recordCount);
        // Limit can't be greater than the maximum number of records assigned to this job
        solrQuery.setRows(recordCount + LIMIT > nrOfOutputRecords ? nrOfOutputRecords - recordCount : LIMIT);
        final QueryResponse response = solrServer.query(solrQuery);
        for (Iterator<SolrDocument> itResults = response.getResults().iterator(); itResults.hasNext(); recordCount++) {
          final Integer occKey = (Integer) itResults.next().getFieldValue(OccurrenceSolrField.KEY.getFieldName());
          // Writes the occurrence record obtained from HBase as Map<String,Object> or null if not found.
          Map<String, Object> occurrenceRecordMap = occurrenceHBaseReader.get(occKey);
          if (occurrenceRecordMap != null) {
            incrementDatasetUsage(datasetUsages, occurrenceRecordMap);
            csvWriter.write(occurrenceRecordMap, HEADER);
          }
        }
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    } catch (SolrServerException e) {
      Throwables.propagate(e);
    } finally {
      closer.close();
      // Unlock the assigned lock.
      lock.unlock();
    }
    return new Result(fileJob, datasetUsages);
  }

  /**
   * Creates a SolrQuery that contains the query parameter as the filter query value.
   */
  private SolrQuery createSolrQuery(String query) {
    SolrQuery solrQuery = new SolrQuery();
    solrQuery.setQuery(SolrConstants.DEFAULT_QUERY);
    if (!Strings.isNullOrEmpty(query)) {
      solrQuery.addFilterQuery(query);
    }
    return solrQuery;
  }

  /**
   * Increments in 1 the number of records coming from the dataset (if any) in the occurrencRecordMap.
   */
  private void incrementDatasetUsage(Map<UUID, Long> datasetUsages, Map<String, Object> occurrenceRecordMap) {
    final Object datasetObjKey = occurrenceRecordMap.get(getHiveField(FieldName.DATASET_KEY));
    if (datasetObjKey != null) {
      final UUID datasetKey = (UUID) datasetObjKey;
      if (datasetUsages.containsKey(datasetKey)) {
        datasetUsages.put(datasetKey, datasetUsages.get(datasetKey) + 1);
      } else {
        datasetUsages.put(datasetKey, 1L);
      }
    }
  }
}
