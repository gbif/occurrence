package org.gbif.occurrence.index.hbase;

import org.gbif.occurrence.index.hadoop.EmbeddedSolrServerBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.index.hbase.IndexingUtils.addDocumentQuietly;
import static org.gbif.occurrence.index.hbase.IndexingUtils.buildOccSolrDocument;
import static org.gbif.occurrence.index.hbase.IndexingUtils.buildOccurrenceScan;


/**
 * Class that creates a Solr Index using reading sequentially a hbase table.
 * The {@link SolrServer} instance is an embedded instance by default.
 */
public class SequentialAsyncOccurrenceIndexBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(SequentialAsyncOccurrenceIndexBuilder.class);

  private static final String KEY_SOURCE_TABLE = "occurrence-index.sourceTable";
  private static final String SOLR_HOME = "solr.home";
  private final Properties props;

  /**
   * Default constructor.
   * 
   * @param props configuration properties
   */
  public SequentialAsyncOccurrenceIndexBuilder(Properties props) {
    this.props = props;
  }

  /**
   * Main method.
   * Requires 3 parameters: <hbaseInputTable> <solrHomeDir> <solrDataDir>.
   * hbaseInputTable: name of the hbase table.
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("Usage: SequentialOccurrenceIndexBuilder <hbaseInputTable> <solrHomeDir> <solrDataDir>");
      return;
    }
    Properties p = new Properties();
    p.setProperty(KEY_SOURCE_TABLE, args[0]);
    p.setProperty(SOLR_HOME, args[1]);
    SequentialAsyncOccurrenceIndexBuilder builder = new SequentialAsyncOccurrenceIndexBuilder(p);
    builder.build();
  }


  /**
   * Builds the Occurrence Solr index.
   */
  private void build() throws IOException {
    HTable table = null;
    ResultScanner scanner = null;
    SolrServer solrServer = EmbeddedSolrServerBuilder.build(props.getProperty(SOLR_HOME));
    int numDocs = 0;
    try {
      Configuration conf = HBaseConfiguration.create();
      table = new HTable(conf, props.getProperty(KEY_SOURCE_TABLE));
      Scan scan = buildOccurrenceScan();
      scan.setCaching(200);
      LOG.info("Initiating indexing job");
      scanner = table.getScanner(scan);

      Iterator<Result> iter = scanner.iterator();
      // The doc instance is re-used
      SolrInputDocument doc = new SolrInputDocument();
      while (iter.hasNext()) {
        buildOccSolrDocument(iter.next(), doc);
        addDocumentQuietly(solrServer, doc);
        numDocs++;
      }
    } finally {
      Closeables.closeQuietly(scanner);
      Closeables.closeQuietly(table);
      commitQuietly(solrServer);
      shutdownSolrServer(solrServer);
      LOG.info("Indexing job has finished, # of documents indexed: {}", numDocs);
    }
  }

  /**
   * Commits changes to the solr server.
   * Exceptions {@link SolrServerException} and {@link IOException} are swallowed.
   */
  private void commitQuietly(SolrServer solrServer) {
    try {
      solrServer.commit();
    } catch (SolrServerException e) {
      LOG.error("Solr error commiting on Solr server", e);
    } catch (IOException e) {
      LOG.error("I/O error commiting on Solr server", e);
    }
  }

  /**
   * Shutdowns the Solr server.
   * This methods works only if server is an {@link EmbeddedSolrServer}.
   */
  private void shutdownSolrServer(SolrServer solrServer) {
    if (solrServer instanceof EmbeddedSolrServer) {
      ((EmbeddedSolrServer) solrServer).shutdown();
    }
  }
}
