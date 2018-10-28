package org.gbif.occurrence.beam.solr;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Pipeline settings and arguments for Solr indexing.
 */
public interface BulkLoadOptions extends PipelineOptions {

  @Description("Name of the Solr collection")
  String getSolrCollection();
  void setSolrCollection(String solrCollection);

  @Description("Solr Zookeeper ensemble, including chroot")
  String getSolrZk();
  void setSolrZk(String solrZk);

  @Description("HBase Zookeeper ensemble")
  String getHbaseZk();
  void setHbaseZk(String hbaseZk);

  @Description("Occurrence table")
  String getTable();
  void setTable(String table);

  @Description("Batch size of documents to be sent to Solr")
  @Default.Integer(10000)
  int getBatchSize();
  void setBatchSize(int batchSize);

  @Description("Maximum number of retries when writing to Solr")
  @Default.Integer(10000)
  int getMaxAttempts();
  void setMaxAttempts(int maxAttempts);

  @Description("Used to discard documents based on key values: doc.key % divisor == keyRemainder")
  @Default.Integer(1)
  int getKeyDivisor();
  void setKeyDivisor(int keyDivisor);

  @Description("Used to discard documents based on key values: doc.key % divisor == keyRemainder")
  @Default.Integer(0)
  int getKeyRemainder();
  void setKeyRemainder(int keyRemainder);
}
