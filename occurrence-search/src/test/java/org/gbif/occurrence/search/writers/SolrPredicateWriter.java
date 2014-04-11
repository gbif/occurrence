/**
 *
 */
package org.gbif.occurrence.search.writers;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.occurrence.search.writer.SolrOccurrenceWriter;

import com.google.common.base.Predicate;


/**
 * Utility class that stores an Occurrence record into a Solr index.
 */
public class SolrPredicateWriter implements Predicate<Occurrence> {


  private final SolrOccurrenceWriter solrOccurrenceWriter;

  /**
   * Default constructor.
   */
  public SolrPredicateWriter(SolrOccurrenceWriter solrOccurrenceWriter) {
    this.solrOccurrenceWriter = solrOccurrenceWriter;
  }

  /**
   * Processes the occurrence object.
   */
  @Override
  public boolean apply(Occurrence input) {
    try {
      solrOccurrenceWriter.update(input);
    } catch (Exception e) {
      return false;
    }
    return true;
  }
}
