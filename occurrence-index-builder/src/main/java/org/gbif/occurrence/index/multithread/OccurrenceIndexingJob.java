package org.gbif.occurrence.index.multithread;

import org.gbif.occurrence.index.hbase.IndexingUtils;

import java.io.IOException;
import java.util.concurrent.Callable;

import com.google.common.io.Closer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.index.hbase.IndexingUtils.buildOccurrenceScan;

/**
 * Job that creates a part of CSV file. The file is generated according to the fileJob field.
 */
class OccurrenceIndexingJob implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceIndexingJob.class);

  private final SolrServer solrServer;
  private final String hTable;
  private final int stopRow;
  private final int startRow;

  /**
   * Default constructor.
   */
  public OccurrenceIndexingJob(int startRow, int stopRow, SolrServer solrServer, String hTable) {
    this.solrServer = solrServer;
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.hTable = hTable;
  }


  /**
   * Executes the job.query and creates a data file that will contains the records from job.from to job.to positions.
   */
  @Override
  public Integer call() throws IOException {
    HTable table = null;
    ResultScanner scanner = null;
    int numDocs = 0;
    // The doc instance is re-used
    SolrInputDocument doc = new SolrInputDocument();
    LOG.info("Initiating indexing job from " + startRow + " to " + stopRow);
    Closer closer = Closer.create();
    try {
      Configuration conf = HBaseConfiguration.create();
      table = new HTable(conf, hTable);
      closer.register(table);
      Scan scan = buildOccurrenceScan();
      scan.setStartRow(Bytes.toBytes(startRow));
      scan.setStopRow(Bytes.toBytes(stopRow));
      scan.setCaching(200);
      scanner = table.getScanner(scan);
      closer.register(scanner);
      for (Result result : scanner) {
        IndexingUtils.buildOccSolrDocument(result, doc);
        IndexingUtils.addDocumentQuietly(solrServer, doc);
        numDocs++;
      }
    } catch (Exception ex) {
      LOG.error("A general error has occurred", ex);
      LOG.error("Last occurrence record visited", doc.getFieldValue("key"));
    } finally {
      closer.close();
      LOG.info("Commiting {} documents", numDocs);
      LOG.info("Indexing job has finished, # of documents indexed: {}", numDocs);
    }
    return numDocs;
  }

}
