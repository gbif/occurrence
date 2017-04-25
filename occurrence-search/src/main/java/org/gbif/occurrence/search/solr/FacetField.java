package org.gbif.occurrence.search.solr;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that defines a faceted field.
 * The field corresponds to the name of it in the index data store or in the model object.
 * The name is the actual name of the facet, this field is used to naming the facet in consistent way without
 * dependencies of the field name. The intended use of name is that it should contain values coming from literal names
 * of a enumerated type (e.g: Enum.name()).
 *
 * moved from common-search, see: https://github.com/gbif/common-search/commit/c9529087d5b34228b045f30323901074218c5d90
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface FacetField {

  /**
   * Valid sort orders.
   */
  public enum SortOrder {
    /**
     * Sort the constraints by count (highest count first).
     */
    COUNT,
    /**
     * Sorted in index order (lexicographic by indexed term).
     * For terms in the ascii range, this will be alphabetically sorted.
     */
    INDEX
  }

  /**
   * Indicates what type of algorithm/method to use when faceting a field.
   */
  public enum Method {

    /**
     * Enumerates all terms in a field, calculating the set intersection of documents that match the term with documents
     * that match the query.
     */
    ENUM,

    /**
     * The facet counts are calculated by iterating over documents that match the query and summing the terms that
     * appear in each document.
     */
    FIELD_CACHE,

    /**
     * Works the same as FIELD_CACHE except the underlying cache data structure is built for each segment of the index
     * individually.
     */
    FIELD_CACHE_SEGMENT
  }

  /**
   * @return the field name
   */
  String field();

  /**
   * Indicates if count of all matching results which have no value for the field should be included.
   *
   * @return flag that indicates missing facet is included
   */
  boolean missing() default false;

  /**
   * @return the name of the facet exactly as SearchParameter enum specifies
   */
  String name();

  /**
   * @return the sort order for this facet field
   */
  SortOrder sort() default SortOrder.COUNT;

  Method method() default Method.FIELD_CACHE;
}
