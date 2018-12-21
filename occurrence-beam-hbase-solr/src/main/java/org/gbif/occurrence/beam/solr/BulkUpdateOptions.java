package org.gbif.occurrence.beam.solr;

import org.apache.beam.sdk.options.Description;

/**
 * Beam option used to perform updates in Solr using a query to filter input data.
 */
interface BulkUpdateOptions extends BulkLoadOptions {

  @Description("Solr to filter input documents")
  String getSolrQuery();
  void setSolrQuery(String solrQuery);

}
