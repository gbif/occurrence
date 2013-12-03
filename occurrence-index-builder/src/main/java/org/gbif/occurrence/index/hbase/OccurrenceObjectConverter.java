/**
 * 
 */
package org.gbif.occurrence.index.hbase;

import org.gbif.occurrence.index.hadoop.SolrDocumentConverter;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.io.IntWritable;
import org.apache.solr.common.SolrInputDocument;


/**
 * Converts a occurrence record into a Solr document.
 */
public class OccurrenceObjectConverter extends SolrDocumentConverter<OccurrenceWritable, IntWritable> {

  @Override
  public Collection<SolrInputDocument> convert(OccurrenceWritable key, IntWritable value) {
    ArrayList<SolrInputDocument> list = new ArrayList<SolrInputDocument>();
    list.add(IndexingUtils.toSolrInputDocument(key));
    return list;
  }
}
