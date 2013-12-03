package org.gbif.occurrence.index.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EmbeddedSolrServerBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSolrServerBuilder.class);

  /**
   * Builds a SolrServer using a home directory and data directory.
   * 
   * @param solrHome
   * @return
   */
  public static EmbeddedSolrServer build(String solrHome) {
    try {
      File solrHomeFile = new File(solrHome);
      // File solrDataDir = new File(dataDir);
      if (!solrHomeFile.exists()) {
        throw new FileNotFoundException(solrHome);
      }
      String solrHomePath = solrHomeFile.getAbsolutePath();
      LOG.info(String.format("Constructed instance information solr.home %s", solrHomePath));
      CoreContainer container = new CoreContainer(solrHomePath);
      container.load();
      System.out.println(container.getAllCoreNames());
      return new EmbeddedSolrServer(container, "");
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static void main(String[] args) {
    SolrServer server = EmbeddedSolrServerBuilder.build("solr/");
    System.out.println(server.toString());
  }
}
