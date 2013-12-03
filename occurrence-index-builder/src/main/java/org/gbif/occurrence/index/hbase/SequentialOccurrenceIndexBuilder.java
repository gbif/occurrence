package org.gbif.occurrence.index.hbase;

import org.gbif.occurrence.index.hadoop.EmbeddedSolrServerBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import com.google.common.io.Closer;
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
public class SequentialOccurrenceIndexBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(SequentialOccurrenceIndexBuilder.class);

  private static final String KEY_SOURCE_TABLE = "occurrence-index.sourceTable";
  private static final String SOLR_HOME = "solr.home";
  private static final String NR_OF_RECORDS = "nr_of_records";
  private final Properties props;

  /**
   * Default constructor.
   * 
   * @param props configuration properties
   */
  public SequentialOccurrenceIndexBuilder(Properties props) {
    this.props = props;
  }

  /**
   * Main method.
   * Requires 3 parameters: <hbaseInputTable> <solrHomeDir> <solrDataDir>.
   * hbaseInputTable: name of the hbase table.
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err
        .println("Usage: SequentialOccurrenceIndexBuilder <hbaseInputTable> <solrHomeDir> <solrDataDir>");
      return;
    }
    Properties p = new Properties();
    p.setProperty(KEY_SOURCE_TABLE, args[0]);
    p.setProperty(SOLR_HOME, args[1]);
    if (args[2] != null) {
      p.setProperty(NR_OF_RECORDS, args[2]);
    }
    SequentialOccurrenceIndexBuilder builder = new SequentialOccurrenceIndexBuilder(p);
    builder.build();
  }


  /**
   * Builds the Occurrence Solr index.
   */
  private void build() throws IOException {
    HTable table = null;
    ResultScanner scanner = null;
    Closer closer = Closer.create();
    SolrServer solrServer = EmbeddedSolrServerBuilder.build(props.getProperty(SOLR_HOME));
    long nrOfRecords = -1;
    if (props.containsKey(NR_OF_RECORDS)) {
      nrOfRecords = Long.parseLong(props.getProperty(NR_OF_RECORDS));
    }
    int numDocs = 0;
    // The doc instance is re-used
    SolrInputDocument doc = new SolrInputDocument();
    LOG.info("Initiating indexing job");
    try {
      Configuration conf = HBaseConfiguration.create();
      table = new HTable(conf, props.getProperty(KEY_SOURCE_TABLE));
      closer.register(table);
      Scan scan = buildOccurrenceScan();
      scan.setCaching(1200);
      scanner = table.getScanner(scan);
      closer.register(scanner);

      Iterator<Result> iter = scanner.iterator();
      while (iter.hasNext() && (nrOfRecords == -1 || numDocs < nrOfRecords)) {
        buildOccSolrDocument(iter.next(), doc);
        addDocumentQuietly(solrServer, doc);
        numDocs++;
      }
    } catch (Exception ex) {
      LOG.error("A general error has occurred", ex);
      LOG.error("Last occurrence record visited", doc.getFieldValue("key"));
    } finally {
      closer.close();
      LOG.info("Commiting {} documents", numDocs);
      commitQuietly(solrServer);
      LOG.info("Commit has finished");
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
