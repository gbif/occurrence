package org.gbif.occurrence.persistence.api;

import org.gbif.dwc.terms.Term;

/**
 * Internal GBIF terms used for processing, fragmenting, crawling, ...
 * These are not exposed in downloads or the public API.
 */
public enum InternalTerm implements Term {
  key,
  identifierCount,
  crawlId,
  fragment,
  fragmentHash,
  fragmentCreated,
  xmlSchema,
  publishingOrgKey;

  public static final String NS = "http://rs.gbif.org/terms/internal/";
  public static final String PREFIX = "int";

  @Override
  public String qualifiedName() {
    return NS + name();
  }

  @Override
  public String simpleName() {
    return name();
  }

  @Override
  public String toString() {
    return PREFIX + ":" + name();
  }

}
