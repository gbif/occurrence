package org.gbif.occurrence.beam.solr;

import org.apache.beam.sdk.options.PipelineOptions;

public interface BulkLoadOptions extends PipelineOptions {
  String getKeyDivisor();
  void setKeyDivisor(String keyDivisor);

  String getKeyRemainder();
  void setKeyRemainder(String keyRemainder);

  String getSolrCollection();
  void setSolrCollection(String solrCollection);
}
